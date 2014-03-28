package storageserver

import (
    "fmt"
	"storageproto"
    "sync"
    "net/rpc"
    "log"
    "hash/crc32"
    "sort"
    "net"
    "time"
    "strconv"
)

type Storageserver struct {
    master string
    ismaster bool
    numnodes int
    portnum int
    nodeid uint32
    lock sync.RWMutex
    kv map[string]string
    kvl map[string][]string
    peers map[uint32]*peer
    peerSortedKeys []uint32 
    ready bool
    registerRequestChan chan *storageproto.RegisterArgs
    registerReplyChan chan *storageproto.RegisterReply
}

func NewStorageserver(master string, numnodes int, portnum int, nodeid uint32) *Storageserver {
	ss := &Storageserver{
        master : master,
        ismaster : false,
        numnodes : numnodes,
        portnum : portnum,
        nodeid : nodeid,
        kv : make(map[string]string),
        kvl : make(map[string][]string),
        peers : make(map[uint32]*peer),
        peerSortedKeys : make([]uint32, 0),
	}
    
    ss.registerRequestChan = make(chan *storageproto.RegisterArgs, 0)
    ss.registerReplyChan = make(chan *storageproto.RegisterReply, 0)
    
    ss.lock.Lock()
    defer ss.lock.Unlock()
    self := NewPeer(nodeid, "127.0.0.1", portnum)
    ss.addPeer(self)
    
    // single mode
    if numnodes == 1 && len(ss.master) > 0 {
        ss.ready = true
        return ss
    }
    
    // cluster mode
    // master node 
    if numnodes > 1 {
		// I'm the master!  That's exciting!
        ss.ismaster = true
        ss.ready = false
        log.Printf("NewStorageserver: I'm master\n")
    } else {
        // Slave
        ss.ismaster = false
        ss.ready = false
        log.Printf("NewStorageserver: I'm slave\n")
    }
    
    go ss.RegisterLoop()
    
	return ss
}

type peer struct {
    nodeid uint32
    address string
    portnum int
    rpcClient *rpc.Client
}

func NewPeer(nodeid uint32, address string, portnum int) *peer {
    p := &peer{
        nodeid : nodeid,
        address : address,
        portnum : portnum,
    }
    return p
}




// You might define here the functions that the locally-linked application
// logic can use to call things like Get, GetList, Put, etc.
// OR, you can have them access the local storage node
// by calling the RPC functions.  Your choice!
func (ss *Storageserver) localGet(key string) (string, bool) {
    ss.lock.RLock()
    defer ss.lock.RUnlock()
    value, ret := ss.kv[key]
    log.Printf("cache: get %s %s", key, value)
    return value, ret
}

func (ss *Storageserver) localPut(key string, value string) bool {
    ss.lock.Lock()
    defer ss.lock.Unlock()
    if _, e := ss.kv[key]; e {
        return false
    } 
    ss.kv[key] = value
    log.Printf("cache: put %s %s", key, value)
    return true
}

func (ss *Storageserver) localGetList(key string) ([]string, bool) {
    ss.lock.RLock()
    defer ss.lock.RUnlock()
    log.Printf("cache: getList key %s", key)
    value, ret := ss.kvl[key]
    return value, ret
}

func (ss *Storageserver) localAppendToList(key string, value string) bool {
    ss.lock.Lock()
    defer ss.lock.Unlock()
    if _, e := ss.kvl[key]; e == false {
       ss.kvl[key] = make([]string, 0)
    }
    ss.kvl[key] = append(ss.kvl[key], value)
    log.Printf("cache: appendToList %s %s", key, value)
    return true
}

func (ss *Storageserver) localRemoveFromList(key string, value string) bool {
    ss.lock.Lock()
    defer ss.lock.Unlock()
    var ret bool
    var l []string
    
    log.Printf("cache: removeFromList %s %s", key, value)
    
    if l, ret = ss.kvl[key]; ret == false {
        return false
    }
    for i, e := range l {
        if e == value {
            l = append(l[:i], l[i+1:]...)
        }
    }
    ss.kvl[key] = l
    return false
}

// peer 
// peer add and dht func
// use nodeid as DHT's key, not actually gen key from name or others.
// todo:the append/sort all method may be improve.
type Bynodeid []uint32
func (a Bynodeid) Len() int {
    return len(a)
}

func (a Bynodeid) Less(i, j int) bool {
    return a[i] < a[j]
}

func (a Bynodeid) Swap(i, j int) {
    a[i], a[j] = a[j], a[i]
}

func (ss *Storageserver) addPeer(p *peer) {
    ss.peerSortedKeys = append(ss.peerSortedKeys, p.nodeid)
    sort.Sort(Bynodeid(ss.peerSortedKeys))
    ss.peers[p.nodeid] = p
}

// if peer is already add, return true
func (ss *Storageserver) checkPeerExist(p *peer) bool {
    if ss.peers[p.nodeid] != nil {
        return true
    }
    return false
}



// input: key of operations.
// scan peer's hashkey(nodeid) from small to big until find the bigger nearest.
var hash = crc32.ChecksumIEEE
func (ss *Storageserver) findPeer(key string) *peer {
    
    if len(ss.peerSortedKeys) == 0 {
        return nil
    }
    hashkey := uint32(hash([]byte(key)))
    log.Printf("findPeer: key %s, hashkey %u", key, hashkey)
    
    for _, nodekey := range ss.peerSortedKeys {
        if nodekey >= hashkey {
            log.Printf("findPeer: get peer: nodeid %d, port %d\n", nodekey, ss.peers[nodekey].portnum)
            return ss.peers[nodekey]
        }
    }
    
    firstnode := ss.peerSortedKeys[0]
    log.Printf("findPeer: get peer: nodeid %d, port %d\n", ss.peers[firstnode].nodeid, ss.peers[firstnode].portnum)
    return ss.peers[firstnode]
}

func (ss *Storageserver) peerGet(p *peer, key string) (string, bool) {
    var ret bool
    if (p.rpcClient == nil) {
        portstr := fmt.Sprintf("%d", p.portnum)
        client, err := rpc.DialHTTP("tcp", net.JoinHostPort(p.address, portstr))
        if (err != nil) {
            log.Fatal("could not connect to server")
            return "", false
        }
        p.rpcClient = client
    }
    
    getargs := &storageproto.GetArgs{
        Key : key,
    }
    getreply := &storageproto.GetReply{}
    
    err := p.rpcClient.Call("StorageRPC.Get", getargs, getreply)
    if (err != nil) {
        log.Fatal("peerGet: rpcclient Get Call failed on node %d", p.nodeid)
        return "", false
    }

    value := getreply.Value
    if (getreply.Status == storageproto.OK) {
        ret = true
    } else {
        ret = false
    }
    return value, ret
}

func (ss *Storageserver) peerPut(p *peer, key string, value string) bool {
    var ret bool
    if (p.rpcClient == nil) {
        portstr := fmt.Sprintf("%d", p.portnum)
        log.Printf("peerPut: address %s, port %s", p.address, portstr)
        client, err := rpc.DialHTTP("tcp", net.JoinHostPort(p.address, portstr))
        if (err != nil) {
            log.Fatal("could not connect to server")
            return false
        }
        p.rpcClient = client
    }
    putargs := &storageproto.PutArgs{
        Key : key,
        Value : value,
    }
    putreply := &storageproto.PutReply{}
    
    err := p.rpcClient.Call("StorageRPC.Put", putargs, putreply)
    if (err != nil) {
        log.Fatal("peerPut: rpcclient Put call failed no node %d", p.nodeid)
        return false
    }
    if (putreply.Status == storageproto.OK) {
        ret = true
    } else {
        ret = false
    }
    return ret
}

func (ss *Storageserver) peerGetList(p *peer, key string) ([]string, bool) {
    var ret bool
    if (p.rpcClient == nil) {
        portstr := fmt.Sprintf("%d", p.portnum)
        client, err := rpc.DialHTTP("tcp", net.JoinHostPort(p.address, portstr))
        if (err != nil) {
            log.Fatal("could not connect to server")
            return nil, false
        }
        p.rpcClient = client
    }
    getargs := &storageproto.GetArgs{
        Key : key,
    }
    getlistreply := &storageproto.GetListReply{}
    
    err := p.rpcClient.Call("StorageRPC.GetList", getargs, getlistreply)
    if (err != nil) {
        log.Fatal("peerPut: rpcclient GetList call failed no node %d", p.nodeid)
        return nil, false
    }
    value := getlistreply.Value
    if (getlistreply.Status == storageproto.OK) {
        ret = true
    } else {
        ret = false
    }
    return value, ret
}

func (ss *Storageserver) peerAppendToList(p *peer, key string, value string) bool {
    var ret bool
    if (p.rpcClient == nil) {
        portstr := fmt.Sprintf("%d", p.portnum)
        client, err := rpc.DialHTTP("tcp", net.JoinHostPort(p.address, portstr))
        if (err != nil) {
            log.Fatal("could not connect to server")
            return false
        }
        p.rpcClient = client
    }
    putargs := &storageproto.PutArgs{
        Key : key,
        Value : value,
    }
    putreply := &storageproto.PutReply{}
    
    err := p.rpcClient.Call("StorageRPC.AppendToList", putargs, putreply)
    if (err != nil) {
        log.Fatal("peerPut: rpcclient AppendToList call failed no node %d", p.nodeid)
        return  false
    }
    if (putreply.Status == storageproto.OK) {
        ret = true
    } else {
        ret = false
    }
    return ret
}

func (ss *Storageserver) peerRemoveFromList(p *peer, key string, value string) bool {
    var ret bool
    if (p.rpcClient == nil) {
        portstr := fmt.Sprintf("%d", p.portnum)
        client, err := rpc.DialHTTP("tcp", net.JoinHostPort(p.address, portstr))
        if (err != nil) {
            log.Fatal("could not connect to server")
            return false
        }
        p.rpcClient = client
    }
    putargs := &storageproto.PutArgs{
        Key : key,
        Value : value,
    }
    putreply := &storageproto.PutReply{}
    
    err := p.rpcClient.Call("StorageRPC.RemoveFromeList", putargs, putreply)
    if (err != nil) {
        log.Fatal("peerPut: rpcclient RemoveToList call failed no node %d", p.nodeid)
        return false
    }
    if (putreply.Status == storageproto.OK) {
        ret = true
    } else {
        ret = false
    }
    return ret
}


// wrapper local and peer operations.
func (ss *Storageserver) Get(key string) (string, bool) {
    // dht lookup
    p := ss.findPeer(key)
    // local
    if (p.nodeid == ss.nodeid) {
        return ss.localGet(key)
    } else {
        // peer
        return ss.peerGet(p, key)
    }
}

func (ss *Storageserver) Put(key string, value string) bool {
    // dht lookup
    p := ss.findPeer(key)
    // local
    if (p.nodeid == ss.nodeid) {
        return ss.localPut(key, value)
    } else {
        // peer
        return ss.peerPut(p, key, value)
    }
}

func (ss *Storageserver) GetList(key string) ([]string, bool) {
    // dht lookup
    p := ss.findPeer(key)
    // local
    if (p.nodeid == ss.nodeid) {
        return ss.localGetList(key)
    } else {
        // peer
        return ss.peerGetList(p, key)
    } 
}

func (ss *Storageserver) AppendToList(key string, value string) bool{
    // dht lookup
    p := ss.findPeer(key)
    // local
    if (p.nodeid == ss.nodeid) {
        return ss.localAppendToList(key, value)
    } else {
        // peer
        return ss.peerAppendToList(p, key, value)
    }
}


func (ss *Storageserver) RemoveFromList(key string, value string) bool{
    // dht lookup
    p := ss.findPeer(key)
    // local
    if (p.nodeid == ss.nodeid) {
        return ss.localRemoveFromList(key, value)
    } else {
        // peer
        return ss.peerRemoveFromList(p, key, value)
    }
}


func (ss *Storageserver)isMaster() bool {
    return ss.ismaster
}

// RPC-able interfaces, bridged via StorageRPC.
// These should do something! :-)
// RPC-able interfaces use by storageserver to get/set/... data that not belong itself,
// so this RPC only get/set/.. from local, not call others RPC again.

func (ss *Storageserver) RegisterLoop() {
//    receiveNum := 0
    
    tick := time.Tick(5 * time.Second)
    
    for {
        select {
        // master receive register request
        case regArgs := <- ss.registerRequestChan:
            log.Printf("RegisterLoop: recv peer add request")
            ss.handleRegisterRequest(regArgs)
        // slave send register request
        case <- tick:
            // FIXME: when client register, still trigger tick
            if !ss.isMaster() && ss.ready == false {
                log.Printf("RegisterLoop: tick")
                ss.sendRegisterRequest()
            }
        }
    }
    log.Printf("RegisterLoop: all peer ready.")
}

func (ss *Storageserver) handleRegisterRequest(req *storageproto.RegisterArgs) {
    if !ss.isMaster() {
        log.Printf("handleRegisterRequest: not master")
        return
    }
    
    log.Printf("handleRegisterRequest")
    clientInfo := req.ClientInfo
    
    hostPort, _ := strconv.Atoi(clientInfo.HostPort)
    nodeID := clientInfo.NodeID
    newPeer := NewPeer(nodeID, "127.0.0.1", hostPort)
    ss.lock.Lock()
    defer ss.lock.Unlock()
    if !ss.checkPeerExist(newPeer) {
        log.Printf("handleRegisterRequest: new node, add it, nodeid %s", nodeID)
        ss.addPeer(newPeer)
    }
    reply := &storageproto.RegisterReply{}
    var clients []storageproto.Client
    if len(ss.peerSortedKeys) == ss.numnodes {
        // all node ready
        log.Printf("handleRegisterRequest: all node ready")
        ss.ready = true
        for _, peer := range ss.peers {
            c := storageproto.Client{
                HostPort : strconv.Itoa(peer.portnum),
                NodeID : peer.nodeid,
            }
            clients = append(clients, c)
        }
        reply.Clients = clients
        reply.Ready = true
    } else {
        reply.Ready = false
    }
    
    ss.registerReplyChan <- reply
}

func (ss *Storageserver) slaveHandleRegister() {

}

func (ss *Storageserver) sendRegisterRequest() (bool, error) {
    client, err := rpc.DialHTTP("tcp", ss.master)
    if err != nil {
        log.Fatal("sendRegisterRequest: create client failed")
    }
    // init RegisterArg
    node := storageproto.Client{
        HostPort : strconv.Itoa(ss.portnum),
        NodeID : ss.nodeid,
    }
    regRequest := &storageproto.RegisterArgs {}
    regRequest.ClientInfo = node
    var regReply storageproto.RegisterReply
    log.Printf("sendRegisterRequest:")
    err = client.Call("StorageRPC.Register", regRequest, &regReply)
    
    if err != nil {
        return false, err
    }
    
    if regReply.Ready == false {
        return false, nil
    }
    
    // ready. add all peers
    ss.lock.Lock()
    defer ss.lock.Unlock()
    for _, node := range regReply.Clients {
        hostport, _ :=  strconv.Atoi(node.HostPort)
        newPeer := NewPeer(node.NodeID, "127.0.0.1", hostport)
        ss.addPeer(newPeer)
    }
    log.Printf("sendRegisterRequest: all node ready")
    ss.ready = true
    
    return true, nil
}

// RegisterRPC is run on master storage. client call it.
func (ss *Storageserver) RegisterRPC(args *storageproto.RegisterArgs, reply *storageproto.RegisterReply) error {
    ss.registerRequestChan <- args
    ret := <-ss.registerReplyChan
    reply.Ready = ret.Ready
    reply.Clients = ret.Clients
    return nil
}

func (ss *Storageserver) GetRPC(args *storageproto.GetArgs, reply *storageproto.GetReply) error {
	// localGet(key string) (string, bool)
    key := args.Key
    value, ret := ss.localGet(key)
    if (ret == true) {
        reply.Value = value
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EKEYNOTFOUND
    }
    return nil
}

func (ss *Storageserver) GetListRPC(args *storageproto.GetArgs, reply *storageproto.GetListReply) error {
    key := args.Key
    value, status := ss.localGetList(key)
    // fixed me
    if (status == true) {
        reply.Value = value
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EKEYNOTFOUND
    }
    return nil
}

func (ss *Storageserver) PutRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) error {
    key := args.Key
    value := args.Value
    log.Printf("Storageserver.PutRPC: call key %s, value %s", key, value)
    ret := ss.localPut(key, value)
    if (ret == true) {
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EPUTFAILED
    }
    return nil
}

func (ss *Storageserver) AppendToListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) error {
    key := args.Key
    value := args.Value
    ret := ss.localAppendToList(key, value)
    if (ret == true) {
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EITEMEXISTS
    }
    return nil
}

func (ss *Storageserver) RemoveFromListRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) error {
    key := args.Key
    value := args.Value
    ret := ss.localRemoveFromList(key, value)
    if (ret == true) {
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EITEMNOTFOUND
    }
    return nil
}


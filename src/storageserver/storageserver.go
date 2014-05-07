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

const (
    QUERY_COUNT_SECONDS = 5 // Request lease if this is the 3rd+
    QUERY_COUNT_THRESH = 3  // query in the last 5 seconds
    LEASE_SECONDS = 5       // Leases are valid for 5 seconds
    LEASE_GUARD_SECONDS = 1 // Servers add a short guard time
)

type LeaseRequestRecord struct {
    Key string
    LeaseClient storageproto.Client
    LeaseBeginTime  int
}
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
    
    hot_kv map[string]string
    hot_kvl map[string][]string
    // record query timestamp, last time before cache
    hot_query_timestamp map[string]int
    // lease time get from peer.
    // when now - hot_query_timestamp < hot_lease, then
    // the kv value is valid in time
    hot_lease map[string]int

    leaselock sync.RWMutex
    // store all wantlease request
    leaseRequestRecords map[string][]*LeaseRequestRecord
    // when key in change progress, not allow lease
    leaseNotAllow map[string]bool
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
        hot_kv : make(map[string]string),
        hot_kvl : make(map[string][]string),
        hot_query_timestamp : make(map[string]int),
        hot_lease : make(map[string]int), 
        leaseRequestRecords : make(map[string][]*LeaseRequestRecord),
        leaseNotAllow : make(map[string]bool),
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

// Lease 

// the owner of key/value ask we to revokelease.
// here remove the key from hot_kv or hot_kvl
// need update hot_query_timestamp hot_lease?
func (ss *Storageserver) revokeLease(key string) bool {
    if _, e := ss.hot_kv[key]; e {
        delete(ss.hot_kv, key)
        return true
    }
    
    if _, e := ss.hot_kvl[key]; e {
        delete(ss.hot_kvl, key)
        return true
    }
    
    // we don't hot cache it, just return true
    return true
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

func (ss *Storageserver) peerGet(p *peer, key string, wantLease bool) (string, bool, storageproto.LeaseStruct) {
    var ret bool
    if (p.rpcClient == nil) {
        portstr := fmt.Sprintf("%d", p.portnum)
        client, err := rpc.DialHTTP("tcp", net.JoinHostPort(p.address, portstr))
        if (err != nil) {
            log.Fatal("could not connect to server")
            return "", false, storageproto.LeaseStruct{}
        }
        p.rpcClient = client
    }
    
    getargs := &storageproto.GetArgs{
        Key : key,
        WantLease : wantLease,
    }
    getargs.LeaseClient = storageproto.Client{
        HostPort : strconv.Itoa(ss.portnum),
        NodeID : ss.nodeid,
    }
    
    getreply := &storageproto.GetReply{}
    
    err := p.rpcClient.Call("StorageRPC.Get", getargs, getreply)
    if (err != nil) {
        log.Fatal("peerGet: rpcclient Get Call failed on node %d", p.nodeid)
        return "", false, storageproto.LeaseStruct{}
    }

    value := getreply.Value
    if (getreply.Status == storageproto.OK) {
        ret = true
    } else {
        ret = false
    }
    return value, ret, getreply.Lease
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

func (ss *Storageserver) peerGetList(p *peer, key string, wantLease bool) ([]string, bool, storageproto.LeaseStruct) {
    var ret bool
    var nillease storageproto.LeaseStruct
    if (p.rpcClient == nil) {
        portstr := fmt.Sprintf("%d", p.portnum)
        client, err := rpc.DialHTTP("tcp", net.JoinHostPort(p.address, portstr))
        if (err != nil) {
            log.Fatal("could not connect to server")
            return nil, false, nillease
        }
        p.rpcClient = client
    }
    getargs := &storageproto.GetArgs{
        Key : key,
        WantLease : wantLease,
    }
    getlistreply := &storageproto.GetListReply{}
    
    err := p.rpcClient.Call("StorageRPC.GetList", getargs, getlistreply)
    if (err != nil) {
        log.Fatal("peerPut: rpcclient GetList call failed no node %d", p.nodeid)
        return nil, false, nillease
    }
    value := getlistreply.Value
    if (getlistreply.Status == storageproto.OK) {
        ret = true
    } else {
        ret = false
    }
    return value, ret, getlistreply.Lease
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

// call when the key is not on cache or expire
// update the hot_query_timestamp, determine wantLease
// check if the query sent in the last 5 seconds
func (ss *Storageserver) checkHot(key string) bool {
    now := time.Now().Second()
    old, ok := ss.hot_query_timestamp[key]

    if ok == false {
        ss.hot_query_timestamp[key] = now
        return false
    }
    log.Printf("checkHot: old %d, now %d", old, now)
    if now - old < 5 {
        ss.hot_query_timestamp[key] = now
        log.Printf("checkHot: key %s hot, try to hotcache it\n", key)
        return true
    }
    ss.hot_query_timestamp[key] = now
    return false
}

func (ss *Storageserver) HotCacheGet(key string) (value string, status bool) {
    // cache on hot
    if value, ok := ss.hot_kv[key]; ok {
        // it is valid, just return
        if time.Now().Second() - ss.hot_query_timestamp[key] < ss.hot_lease[key] {
            log.Printf("HotCacheGet key %s, value %s\n", key, value)
            return value, ok
        }
        // not valid, expire it
        delete(ss.hot_kv, key)
        ss.hot_query_timestamp[key] = time.Now().Second()
        return "", false
    }
    return "", false
}

// wrapper local and peer operations.
func (ss *Storageserver) Get(key string) (string, bool) {
    log.Printf("Get: ")
    // dht lookup
    p := ss.findPeer(key)
    // local
    if (p.nodeid == ss.nodeid) {
        return ss.localGet(key)
    }
    
    // get from cache
    cvalue, cstatus := ss.HotCacheGet(key)
    if cstatus {
        return cvalue, cstatus
    }
    
    // not on cache, need cache?
    wantLease := ss.checkHot(key)
    
    value, status, lease := ss.peerGet(p, key, wantLease)
    
    if wantLease && lease.Granted {
        ss.hot_kv[key] = value
        ss.hot_lease[key] = lease.ValidSeconds
    }
    
    return value, status
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

func (ss *Storageserver) HotCacheGetList(key string) (vl []string, status bool) {
    // cache on hot
    if vl, ok := ss.hot_kvl[key]; ok {
        // it is valid, just return
        if time.Now().Second() - ss.hot_query_timestamp[key] < ss.hot_lease[key] {
            log.Printf("HotCacheGetList key %s, valuelist %s\n", key, vl)
            return vl, ok
        }
        // not valid, expire it
        delete(ss.hot_kvl, key)
        ss.hot_query_timestamp[key] = time.Now().Second()
        return nil, false
    }
    return nil, false
}

func (ss *Storageserver) GetList(key string) ([]string, bool) {
    // dht lookup
    p := ss.findPeer(key)
    // local
    if (p.nodeid == ss.nodeid) {
        return ss.localGetList(key)
    }
    
    // get from cache
    cvl, cstatus := ss.HotCacheGetList(key)
    if cstatus {
        return cvl, cstatus
    }
    
    // not on cache, need hot cache?
    wantLease := ss.checkHot(key)
    
    // peer with wantLease
    vl, status, lease := ss.peerGetList(p, key, wantLease)
    
    if wantLease && lease.Granted {
        ss.hot_kvl[key] = vl
        ss.hot_lease[key] = lease.ValidSeconds
    }
    return vl, status     
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

func (ss *Storageserver) RecordWantLeaseRequest(args *storageproto.GetArgs) error {
    // add it to leaseRequestRecords
    now := time.Now().Second()
    leaseRequest := &LeaseRequestRecord {
        Key : args.Key,
        LeaseClient : args.LeaseClient,
        LeaseBeginTime : now,
    }
    // TODO: check wether records exist
    key := args.Key
    ss.leaseRequestRecords[key] = append(ss.leaseRequestRecords[key], leaseRequest)
    return nil
}

func (ss *Storageserver) GetRPC(args *storageproto.GetArgs, reply *storageproto.GetReply) error {
	// localGet(key string) (string, bool)
    key := args.Key
    value, ret := ss.localGet(key)
    if ret == true {
        reply.Value = value
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EKEYNOTFOUND
    }
    
    // lease
    if !args.WantLease {
        return nil
    } 
    
    reply.Lease = storageproto.LeaseStruct{
        Granted : true,
        ValidSeconds : LEASE_SECONDS,
    }
    ss.RecordWantLeaseRequest(args)
    
    return nil
}

func (ss *Storageserver) GetListRPC(args *storageproto.GetArgs, reply *storageproto.GetListReply) error {
    key := args.Key
    value, status := ss.localGetList(key)
    // fixed me
    if status == true {
        reply.Value = value
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EKEYNOTFOUND
    }
    
    // lease
    if !args.WantLease {
        return nil
    } 
    
    reply.Lease = storageproto.LeaseStruct{
        Granted : true,
        ValidSeconds : LEASE_SECONDS,
    }
    ss.RecordWantLeaseRequest(args)
    
    return nil
}

func (ss *Storageserver) PutRPC(args *storageproto.PutArgs, reply *storageproto.PutReply) error {
    key := args.Key
    value := args.Value
    log.Printf("Storageserver.PutRPC: call key %s, value %s", key, value)
    ret := ss.localPut(key, value)
    if ret == true {
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
    if ret == true {
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
    if ret == true {
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EITEMNOTFOUND
    }
    return nil
}

func (ss *Storageserver) RevokeLeaseRPC(args *storageproto.RevokeLeaseArgs, reply *storageproto.RevokeLeaseReply) error {
    key := args.Key
    ret := ss.revokeLease(key)
    if ret == true {
        reply.Status = storageproto.OK
    } else {
        reply.Status = storageproto.EREVOKEFAILED
    }
    return nil
}


package main
//package server

import (
	"net/rpc"
	"net/http"
	"net"
	"log"
	"fmt"
	"tribproto"
	"flag"
    "time"
    "strconv"
    "strings"
    "storageserver"
    "storagerpc"
)

type Tribserver struct {
    ss *storageserver.Storageserver
    createUserChan chan string
    createUserReplyChan chan bool
    addSubscripChan chan *tribproto.SubscriptionArgs
    addSubscripReplyChan chan int
    removeSubscripChan chan *tribproto.SubscriptionArgs
    removeSubscripReplyChan chan int
    getSubscripChan chan *tribproto.GetSubscriptionsArgs
    getSubscripReplyChan chan *tribproto.GetSubscriptionsReply
    postTribbleChan chan *tribproto.PostTribbleArgs
    postTribbleReplyChan chan int
    getTribbleChan chan string
    getTribbleReplyChan chan *tribproto.GetTribblesReply
    getTribbleBySubscriptionChan chan string
    getTribbleBySubscriptionReplyChan chan *tribproto.GetTribblesReply
}

func NewTribserver(ss *storageserver.Storageserver) *Tribserver {
	ts := &Tribserver{}
   
    ts.ss = ss
    ts.createUserChan = make(chan string)
    ts.createUserReplyChan = make(chan bool)
    ts.addSubscripChan = make(chan *tribproto.SubscriptionArgs)
    ts.addSubscripReplyChan = make(chan int)
    ts.removeSubscripChan = make(chan *tribproto.SubscriptionArgs)
    ts.removeSubscripReplyChan = make(chan int)
    ts.getSubscripChan = make(chan *tribproto.GetSubscriptionsArgs)
    ts.getSubscripReplyChan = make(chan *tribproto.GetSubscriptionsReply)
    ts.postTribbleChan = make(chan *tribproto.PostTribbleArgs)
    ts.postTribbleReplyChan = make(chan int)
    ts.getTribbleChan = make(chan string)
    ts.getTribbleReplyChan = make(chan *tribproto.GetTribblesReply)
    ts.getTribbleBySubscriptionChan = make(chan string)
    ts.getTribbleBySubscriptionReplyChan = make(chan *tribproto.GetTribblesReply)
        
    go ts.loop()
	return ts
}


func (ts *Tribserver) handleUserCreate(userId string) {
    userKey := "user-" + userId
    ret := ts.ss.Put(userKey, "exist")
    ts.createUserReplyChan <- ret
}

func (ts *Tribserver) CreateUser(args *tribproto.CreateUserArgs, reply *tribproto.CreateUserReply) error {
	// Set responses by modifying the reply structure, like:
	// reply.Status = tribproto.EEXISTS
	// Note that OK is the default;  you don't need to set it explicitly
    ts.createUserChan <- args.Userid
    ret := <- ts.createUserReplyChan
    if ret == false {
        reply.Status = tribproto.EEXISTS
    }
    log.Printf("createUser")
	return nil
}

func (ts *Tribserver) CheckUserExist(userId string) bool {
    userKey := "user-" + userId
    value, ret := ts.ss.Get(userKey)
    if ret == true && value == "exist" {
        return true
    }
    return false
}

func (ts *Tribserver) handleAddSubscrip(userId string, targetId string) {
    var ret int
    // check user and targetId
    if !ts.CheckUserExist(userId) {
        ret = tribproto.ENOSUCHUSER
    } else if !ts.CheckUserExist(targetId) {
        ret = tribproto.ENOSUCHTARGETUSER
    } else {
        subscripKey := "subscrip-" + userId
        log.Printf("handleAddSubscrip: appendToList subscripKey %s targetId %s", subscripKey, targetId)
        ok := ts.ss.AppendToList(subscripKey, targetId)
        if ok {
            ret = tribproto.OK
        }
    }
    ts.addSubscripReplyChan <- ret
}

func (ts *Tribserver) AddSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) error {
	log.Printf("AddSubscription call")
    ts.addSubscripChan <- args
    ret := <- ts.addSubscripReplyChan
    reply.Status = ret
    log.Printf("AddSubscription end")
    return nil
}

func (ts *Tribserver) handleRemoveSubsrip(userId string, targetId string) {
    var ret int 
    if !ts.CheckUserExist(userId) {
        ret = tribproto.ENOSUCHUSER
    } else if !ts.CheckUserExist(targetId) {
        ret = tribproto.ENOSUCHTARGETUSER
    } else {
        subscripKey := "subscrip-" + userId
        ok := ts.ss.RemoveFromList(subscripKey, targetId)
        if ok {
            ret = tribproto.OK
        }
    }
    ts.removeSubscripReplyChan <- ret
}

func (ts *Tribserver) RemoveSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) error {
	log.Printf("RemoveSubscription call")
    ts.removeSubscripChan <- args
    ret := <- ts.removeSubscripReplyChan
    reply.Status = ret
    log.Printf("RemoveSubscription end")
    return nil
}

func (ts *Tribserver) handleGetsubscrip(userId string) {
    ret := &tribproto.GetSubscriptionsReply{}
    if !ts.CheckUserExist(userId) {
        ret.Status = tribproto.ENOSUCHUSER
    } else {
        subscripKey := "subscrip-" + userId
        userIds, _ := ts.ss.GetList(subscripKey)
        ret.Userids = userIds
    }
    ts.getSubscripReplyChan <- ret
}

func (ts *Tribserver) GetSubscriptions(args *tribproto.GetSubscriptionsArgs, reply *tribproto.GetSubscriptionsReply) error {
	log.Printf("GetSubscriptions call")
    ts.getSubscripChan <- args
    reply = <- ts.getSubscripReplyChan
    log.Printf("GetSubscriptions end")
    return nil
}

// post-userid: postid, postid
// postid: dga:post-23ac9138d7
func (ts *Tribserver) handlePostTribble(userId string, content string) {
    var ret int
    if !ts.CheckUserExist(userId) {
        ret = tribproto.ENOSUCHUSER
        ts.postTribbleReplyChan <- ret
        return
    }
    
    userPostKey := "post-" + userId
    postId := userId + ":post-" + strconv.FormatInt(time.Now().UnixNano(), 10)
    ts.ss.Put(postId, content)
    ts.ss.AppendToList(userPostKey, postId)
    // FixMe:
    ts.postTribbleReplyChan <- ret
}

func (ts *Tribserver) PostTribble(args *tribproto.PostTribbleArgs, reply *tribproto.PostTribbleReply) error {
	log.Printf("PostTribble call")
    ts.postTribbleChan <- args
    ret := <- ts.postTribbleReplyChan
    reply.Status = ret
    log.Printf("PostTribble end")
    return nil
}

func (ts *Tribserver) getTribblesByUserId(userId string)  ([]tribproto.Tribble, int) {
    tribbles := make([]tribproto.Tribble, 0)
    status := tribproto.OK
    
    postUserId := "post-" + userId
    tribbleIds, _ := ts.ss.GetList(postUserId)
    
    for _, tribbleId := range tribbleIds {
        t := &tribproto.Tribble{}
        t.Userid = userId
        postedStr := strings.Split(tribbleId, "-")[1]
        t.Posted, _ = strconv.ParseInt(postedStr, 10, 64)
        t.Contents, _ = ts.ss.Get(tribbleId)
        tribbles = append(tribbles, *t)
    }
    
    return tribbles, status
}

func (ts *Tribserver) handleGetTribles(userId string) {
    reply := &tribproto.GetTribblesReply{}
    tribbles, status := ts.getTribblesByUserId(userId)
    reply.Status = status
    reply.Tribbles = tribbles
    ts.getTribbleReplyChan <- reply
}

func (ts *Tribserver) GetTribbles(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) error {
	log.Printf("GetTribbles call")
    ts.getTribbleChan <- args.Userid
    ret := <- ts.getTribbleReplyChan
    reply.Status = ret.Status
    reply.Tribbles = ret.Tribbles
    log.Printf("GetTribbles end")
    return nil
}

func (ts *Tribserver) handleGetTribblesBySubscrip(userId string) {
    log.Printf("handleGetTribblesBySubscrip: userId %s", userId)
    var tribbles []tribproto.Tribble
    subscripKey := "subscrip-" + userId
    subscrips, _ := ts.ss.GetList(subscripKey)
    
    for _, targetId := range subscrips {
        targetTribbles, _ := ts.getTribblesByUserId(targetId)
        log.Printf("handleGetTribblesBySubscrip: userId %s targetId %s", userId, targetId)
        for _, t := range targetTribbles {
            tribbles = append(tribbles, t)
        }
    }
    reply := &tribproto.GetTribblesReply{}
    reply.Status = tribproto.OK
    reply.Tribbles = tribbles
    ts.getTribbleBySubscriptionReplyChan <- reply
}

func (ts *Tribserver) GetTribblesBySubscription(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) error {
	log.Printf("GetTribblesBySubscription call")
    ts.getTribbleBySubscriptionChan <- args.Userid
    ret := <- ts.getTribbleBySubscriptionReplyChan
    reply.Status = ret.Status
    reply.Tribbles = ret.Tribbles
    log.Printf("GetTribblesBySubscription end")
    return nil
}

func (ts *Tribserver) loop() {
    for {
        select {
        case userId := <- ts.createUserChan:
            ts.handleUserCreate(userId)
        case subscripArgs := <- ts.addSubscripChan:
            ts.handleAddSubscrip(subscripArgs.Userid, subscripArgs.Targetuser)
        case removescripArgs := <- ts.removeSubscripChan:
            ts.handleRemoveSubsrip(removescripArgs.Userid, removescripArgs.Targetuser)
        case getSubscriArgs := <- ts.getSubscripChan:
            ts.handleGetsubscrip(getSubscriArgs.Userid)
        case postTribbleArgs := <- ts.postTribbleChan:
            ts.handlePostTribble(postTribbleArgs.Userid, postTribbleArgs.Contents)
        case getTribbleUserid := <- ts.getTribbleChan:
            ts.handleGetTribles(getTribbleUserid)
        case getTribbleBySubscriUserid := <- ts.getTribbleBySubscriptionChan:
            ts.handleGetTribblesBySubscrip(getTribbleBySubscriUserid)
        }
    }
}

var portnum *int = flag.Int("port", 9009, "port # to listen on")
var storageMasterNodePort *string = flag.String("master", "", "Storage master node. Defaults to its own port.")
var numNodes *int = flag.Int("N", 0, "Become the master. Specifies the number of nodes in the system, including the master.")
var nodeID *uint = flag.Uint("id", 0, "The node ID to use for consistent hashing. Should be a 32 bit number.")


func main() {
	flag.Parse()
    if (*storageMasterNodePort == "") {
        // Single node execution
        *storageMasterNodePort = fmt.Sprint("localhost:%d", *portnum)
        if (*numNodes == 0) {
            *numNodes = 1
            log.Println("Self-masterg. setting nodes to 1")
        }
    }
	log.Printf("Server starting on port %d\n", *portnum);
    ss := storageserver.NewStorageserver(*storageMasterNodePort, *numNodes, *portnum, uint32(*nodeID))
	ts := NewTribserver(ss)
	rpc.Register(ts)
    srpc := storagerpc.NewStorageRPC(ss)
    rpc.Register(srpc)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}

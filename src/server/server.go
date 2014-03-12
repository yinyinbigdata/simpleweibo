//package main
package server

import (
	"net/rpc"
	"net/http"
	"net"
	"log"
	"fmt"
	"tribproto"
	"flag"
)

type Tribserver struct {
    storage Cache
    createUserChan chan string
    createUserReplyChan chan bool
    addSubcripChan chan *tribproto.SubscriptionArgs
    addSubcripReplyChan chan int
}

func NewTribserver() *Tribserver {
	ts := &Tribserver{
	    storage : NewCache(),
        createUserChan : make(chan string),
        createUserReplyChan : make(chan int),
	}
    
    go ts.loop()
	return ts
}

func (ts *Tribserver) loop() {
    for {
        select {
        case userId := <- createUser:
            ts.handleUserCreate(serId)
        case subscripargs := <- addSubcripChan:
            ts.handleAddSubcrip(subscripargs.Userid, subscripargs.Targetuser)
        }
    }
}

func (ts *Tribserver) handleUserCreate(userId string) {
    userKey := "user-" + userId
    ret := ts.storage.put(userKey, "exist")
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
	return nil
}

func (ts *Tribserver) CheckUserExist(userId string) bool {
    userKey := "user-" + userId
    value, ret := ts.storage.get(userKey)
    if ret == true && value == "exist" {
        return true
    }
    return false
}

func (ts *TRibserver) handleAddSubcrip(userId string, targetId string) {
    var ret int
    // check user and targetId
    if !ts.CheckUserExist(userId) {
        ret = tribproto.ENOSUCHUSER
    } else if !ts.CheckUserExist(targetId) {
        ret = tribproto.ENOSUCHTARGETUSER
    } else {
        subcripKey := "subcrip-" + userId
        ok := ts.storage.appendToList(subcripKey, targetId)
        if ok {
            ret = tribproto.OK
        }
    }
    ts.addSubcripReplyChan <- ret
}

func (ts *Tribserver) AddSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) error {
	ts.addSubcripChan <- args
    ret := <- ts.addSubcripReplyChan
    reply.Status = ret
    return nil
}

func (ts *Tribserver) RemoveSubscription(args *tribproto.SubscriptionArgs, reply *tribproto.SubscriptionReply) error {
	return nil
}

func (ts *Tribserver) GetSubscriptions(args *tribproto.GetSubscriptionsArgs, reply *tribproto.GetSubscriptionsReply) error {
	return nil
}

func (ts *Tribserver) PostTribble(args *tribproto.PostTribbleArgs, reply *tribproto.PostTribbleReply) error {
	return nil
}

func (ts *Tribserver) GetTribbles(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) error {
	return nil
}

func (ts *Tribserver) GetTribblesBySubscription(args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) error {
	return nil
}

var portnum *int = flag.Int("port", 9009, "port # to listen on")

func main() {
	flag.Parse()
	log.Printf("Server starting on port %d\n", *portnum);
	ts := NewTribserver()
	rpc.Register(ts)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}

package main

import (
	"testing"
	_ "net/rpc"
	_ "net"
	"tribproto"
	"tribbleclient"
	"os"
	"runjob"
	"fmt"
	"math/rand"
	"time"
	"runtime"
    "log"
)

const (
	START_PORT_NUMBER = 10000
)

func startServerGeneral(t *testing.T, port int, master_port int, lognum int, numservers int) *runjob.Job {
	// Semantics:  If master_port == port, then supply numservers param,
	// don't set -master.
	// Otherwise, omit -N and invoke with master:port
	runjob.Vlogf(1, "Starting server")
	logname := fmt.Sprintf("server.%d.log", lognum)
	logfile, err := os.OpenFile(logname, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("could not open log file")
	}
	portstr := fmt.Sprintf("-port=%d", port)
	var configstr string
	if port == master_port {
		configstr = fmt.Sprintf("-N=%d", numservers)
	} else {
		configstr = fmt.Sprintf("-master=localhost:%d", master_port)
	}
	server := runjob.NewJob("server", 0.0, logfile, portstr, configstr)
	if server == nil {
		t.Fatalf("Could not start server!")
	}
	return server
}

func startServer(t *testing.T, port int) *runjob.Job {
	server := startServerGeneral(t, port, port, 0, 1) 
	if (server != nil) {
		runjob.Delay(2)
		if (!server.Running()) {
			t.Fatalf("Could not start server!")
		}
	}
	return server
}

func startNServers(t *testing.T, startport int, numservers int) []*runjob.Job {
	ret := make([]*runjob.Job, numservers)
	ret[0] = startServerGeneral(t, startport, startport, 0, numservers)
	for i := 1; i < numservers; i++ {
		ret[i] = startServerGeneral(t, startport+i, startport, i, 0)
	}
	runjob.Delay(3)
	for i := 0; i < numservers; i++ {
		if !ret[i].Running() {
			t.Fatalf("could not start server ", i)
		}
	}
	return ret
}

func killServers(servers []*runjob.Job) {
	for _, server := range servers {
		if server != nil {
			runjob.Vlogf(1, "Killing server")
			server.Kill()
		}
	}
	for _, server := range servers {
		if (server != nil) {
			for (server.Running()) {
				runjob.Delay(0.1)
			}
		}
	}
}

func killServer(server *runjob.Job) {
	s := []*runjob.Job{server}
	killServers(s)
}

func randportno() int {
	rand.Seed(time.Now().UnixNano())
	randsize := 60000 - START_PORT_NUMBER
	return (START_PORT_NUMBER + (rand.Int() % randsize))
}

func TestStartServer(t *testing.T) {
	port :=  randportno()
	server := startServer(t, port)
	killServer(server)
}

func dialServer(t *testing.T) (*runjob.Job, *tribbleclient.Tribbleclient, int) {
	port := randportno()
	server := startServer(t, port)
	tc := getClient(t, port)
	if (tc == nil) { // failure logged in getClient already
		killServer(server)
		return nil, nil, 0
	}
	return server, tc, port
}

func getClient(t *testing.T, port int) (*tribbleclient.Tribbleclient) {
	tc, err := tribbleclient.NewTribbleclient("localhost", fmt.Sprint(port))
	if (err != nil || tc == nil) {
		t.Fatalf("Could not dial server!")
		return nil
	}
	return tc
}

func TestDialServer(t *testing.T) {
	server, client, _ := dialServer(t)
	client.Close()
	killServer(server)
}

func TestCreateUser(t *testing.T) {
	server, client, _ := dialServer(t)
	defer killServer(server)
	defer client.Close()

	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
}

func TestCreateTwice(t *testing.T) {
	server, client, _ := dialServer(t)
	defer killServer(server)
	defer client.Close()

	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	status, err = client.CreateUser("dga")
	if (status != tribproto.EEXISTS || err != nil) {
		t.Fatalf("Incorrect return code from trying to duplicate user")
	}
}

func signalDone(c *chan int) {
	if (c != nil) {
		*c <- 1
	}
}

func createSlam(t *testing.T, client *tribbleclient.Tribbleclient, c *chan int) {
	defer signalDone(c)
	for i := 0; i < 1000; i++ {
		status, err := client.CreateUser("dga")
		if (status != tribproto.EEXISTS || err != nil) {
			t.Fatalf("Incorrect return code from trying to duplicate user")
		}
	}
}

func createUsers(t *testing.T, c *tribbleclient.Tribbleclient, users []string) error {
	for _, u := range users {
		status, err := c.CreateUser(u)
		if (status != tribproto.OK || err != nil) {
			t.Fatalf("Could not create user", u)
			return err
		}
	}
	return nil
}

func TestSubUnsubSlam(t *testing.T) {
	server, client, _ := dialServer(t)
	defer killServer(server)
	defer client.Close()

	users := []string{"dga", "bryant"}
	err := createUsers(t, client, users)
	if (err == nil) {
		subUnsubSlam(t, client, nil, "dga", "bryant")
	}
}
	

func TestCreateSlam(t *testing.T) {
	server, client, _ := dialServer(t)
	defer killServer(server)
	defer client.Close()

	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	createSlam(t, client, nil)
}

func TestConcurrentCreateSlam(t *testing.T) {
	oldprocs := runtime.GOMAXPROCS(-1)
	oldprocs = runtime.GOMAXPROCS(oldprocs+1)
	defer runtime.GOMAXPROCS(oldprocs)
	c := make(chan int)
	server, client, port := dialServer(t)
	defer killServer(server)
	defer client.Close()

	status, err := client.CreateUser("dga")
	if (status != tribproto.OK || err != nil) {
		t.Fatalf("Could not create user")
	}
	cli2 := getClient(t, port)
	if (cli2 == nil) {
		t.Fatalf("No client!  Eek!")
	}
	defer cli2.Close()
	go createSlam(t, client, &c)
	createSlam(t, cli2, nil)
	<- c
}

// returns old value of GOMAXPROCS
func add_N_procs(n int) int {
	curprocs := runtime.GOMAXPROCS(-1)
	return runtime.GOMAXPROCS(curprocs+n)
}

func TestEvil(t *testing.T) {
	defer runtime.GOMAXPROCS(add_N_procs(1))
	//c := make(chan int)
	server, cli1, port := dialServer(t)
	defer killServer(server)
	defer cli1.Close()

	cli2 := getClient(t, port)
	if (server == nil || cli1 == nil || cli2 == nil) {
		t.Fatalf("Setup failed")
		return
	}
	defer cli2.Close()

	subs, s, e := cli1.GetSubscriptions("dga")
	if (e != nil || s != tribproto.ENOSUCHUSER || len(subs) != 0) {
		t.Fatalf("non-user GetSubscriptions did not fail properly")
	}

	subto := []string{"bryant", "imoraru", "rstarzl"}
	cli1.CreateUser("dga")
	for _, sub := range subto {
		cli1.CreateUser(sub)
		cli2.AddSubscription("dga", sub)
	}
	subs, s, e = cli1.GetSubscriptions("dga")
	if (len(subs) != 3) {
		t.Fatalf("Subscription test failed")
	}
}

func subUnsubSlam(t *testing.T, client *tribbleclient.Tribbleclient, c *chan int, user string, target string) {
	defer signalDone(c)
	for i := 0; i < 300; i++ {
		status, err := client.AddSubscription(user, target)
		if (status != tribproto.OK || err != nil) {
			t.Fatalf("AddSubscription failed")
		}
		status, err = client.AddSubscription(user, target)
		if (status != tribproto.EEXISTS || err != nil) {
			t.Fatalf("AddSubscription did not fail when it should have")
		}
		status, err = client.RemoveSubscription(user, target)
		if (status != tribproto.OK || err != nil) {
			t.Fatalf("RemoveSubscription failed")
		}
	}
}

func testNServers(t *testing.T, num_servers int) {
	portbase := randportno()
	servers := startNServers(t, portbase, num_servers)
	defer killServers(servers)
    // Fixed me: node take 2 seconds to regist ...
    log.Printf("sleep 10 seconds")
    runjob.Delay(10)
	clients := make([]*tribbleclient.Tribbleclient, num_servers)
	for i := 0; i < num_servers; i++ {
		clients[i] = getClient(t, portbase+i)
		if (clients[i] == nil) {
			t.Fatalf("Could not create client ", i)
		}
		defer clients[i].Close()
	}

	users := []string{"dga", "bryant"}
	err := createUsers(t, clients[0], users)
	if (err != nil) {
		t.Fatalf("Could not create users")
	}
	clients[0].AddSubscription("dga", "bryant")

	for i := 0; i < num_servers; i++ {
		subs, _, _ := clients[i].GetSubscriptions("dga")
        log.Printf("client %d get subs len %d ", i, len(subs))
		if (len(subs) != 1 || subs[0] != "bryant") {
			t.Fatalf("TwoServer subscription test failed")
		}
	}
}

func TestTwoServers(t *testing.T) {
	testNServers(t, 2)
}

func TestSixServers(t *testing.T) {
    testNServers(t, 6)
}

func TestCache(t *testing.T) {
    num_servers := 6
	portbase := randportno()
	servers := startNServers(t, portbase, num_servers)
	defer killServers(servers)
    // Fixed me: node take 2 seconds to regist ...
    log.Printf("sleep 10 seconds")
    runjob.Delay(10)
	clients := make([]*tribbleclient.Tribbleclient, num_servers)
	for i := 0; i < num_servers; i++ {
		clients[i] = getClient(t, portbase+i)
		if (clients[i] == nil) {
			t.Fatalf("Could not create client ", i)
		}
		defer clients[i].Close()
	}

	users := []string{"dga", "bryant"}
	err := createUsers(t, clients[0], users)
	if (err != nil) {
		t.Fatalf("Could not create users")
	}
	clients[0].AddSubscription("dga", "bryant")

	for i := 0; i < num_servers; i++ {
		subs, _, _ := clients[i].GetSubscriptions("dga")
        log.Printf("client %d get subs len %d ", i, len(subs))
		if (len(subs) != 1 || subs[0] != "bryant") {
			t.Fatalf("TwoServer subscription test failed")
		}
	}
    // test hot cache
    // get it in 5 seconds, so it will put in hotcache
	for i := 0; i < num_servers; i++ {
		subs, _, _ := clients[i].GetSubscriptions("dga")
        log.Printf("client %d get subs len %d ", i, len(subs))
		if (len(subs) != 1 || subs[0] != "bryant") {
			t.Fatalf("TwoServer subscription test failed")
		}
	}
    // get it again, it will return from hotcache
	for i := 0; i < num_servers; i++ {
		subs, _, _ := clients[i].GetSubscriptions("dga")
        log.Printf("client %d get subs len %d ", i, len(subs))
		if (len(subs) != 1 || subs[0] != "bryant") {
			t.Fatalf("TwoServer subscription test failed")
		}
	}
}
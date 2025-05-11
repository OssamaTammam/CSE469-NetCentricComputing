package kvservice

import (
	"fmt"
	"net/rpc"
	"strconv"
	"sysmonitor"
	"time"
)

// import "time"
// import "crypto/rand"
// import "math/big"

type KVClient struct {
	monitorClnt *sysmonitor.Client

	// view provides information about which is primary, and which is backup.
	// Use updateView() to update this view when doing get and put as needed.
	view          sysmonitor.View
	id            string // should be generated to be a random string
	nextRequestId int64
}

func MakeKVClient(monitorServer string) *KVClient {
	client := new(KVClient)
	client.monitorClnt = sysmonitor.MakeClient("", monitorServer)
	client.view = sysmonitor.View{} // An empty view.

	// ToDo: Generate a random id for the client.
	// ==================================
	client.id = strconv.FormatInt(nrand(), 10)
	//====================================

	return client
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// You can use this method to update the client's view when needed during get and put operations.
func (client *KVClient) updateView() {
	view, _ := client.monitorClnt.Get()
	client.view = view
}

// Fetch a key's value from the current primary via an RPC call.
// You can get the primary from the client's view.
// If the key was never set, "" is expected.
// This must keep trying until it gets a response.
func (client *KVClient) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientId:  client.id,
		RequestId: client.nextRequestId,
	}
	reply := GetReply{}

	for {
		client.updateView()

		DPrintf("Client %v: Sending Get[%v] to server %v\n", client.id, args.Key, client.view.Primary)
		success := call(client.view.Primary, "KVServer.Get", &args, &reply)
		if success && (reply.Err == OK || reply.Err == ErrNoKey) {
			break
		}

		DPrintf("Client %v: Request Get[%v] failed retrying in %v\n", client.id, args.Key, sysmonitor.PingInterval)
		time.Sleep(sysmonitor.PingInterval)
	}

	client.nextRequestId++
	DPrintf("Client %v: Get[%v] request to server %v succeeded returned %v\n", client.id, args.Key, client.view.Primary, reply.Value)
	return reply.Value
}

// This should tell the primary to update key's value through an RPC call.
// must keep trying until it succeeds.
// You can get the primary from the client's current view.
func (client *KVClient) PutAux(key string, value string, dohash bool) string {
	args := PutArgs{
		Key:       key,
		Value:     value,
		DoHash:    dohash,
		ClientId:  client.id,
		RequestId: client.nextRequestId,
	}
	reply := PutReply{}

	for {
		client.updateView()

		DPrintf("Client %v: Sending Put[%v]=%v to server %v\n", client.id, args.Key, args.Value, client.view.Primary)
		success := call(client.view.Primary, "KVServer.Put", &args, &reply)
		if success && reply.Err == OK {
			break
		}

		DPrintf("Client %v: Request Put[%v]=%v failed retrying in %v\n", client.id, args.Key, args.Value, sysmonitor.PingInterval)
		time.Sleep(sysmonitor.PingInterval)
	}

	client.nextRequestId++

	DPrintf("Client %v: Put[%v]=%v request to server %v succeeded\n", client.id, args.Key, args.Value, client.view.Primary)

	if dohash {
		return reply.PreviousValue
	}

	return string(reply.Err)
}

// Both put and puthash rely on the auxiliary method PutAux. No modifications needed below.
func (client *KVClient) Put(key string, value string) {
	client.PutAux(key, value, false)
}

func (client *KVClient) PutHash(key string, value string) string {
	v := client.PutAux(key, value, true)
	return v
}

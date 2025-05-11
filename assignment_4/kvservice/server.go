package kvservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"sysmonitor"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Concurrent KVStore struct
type KVStore struct {
	store map[string]string
	mu    sync.RWMutex
}

// Init the store
func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string]string),
	}
}

// Safely put
func (kvStore *KVStore) Put(key string, value string) {
	kvStore.mu.Lock()
	defer kvStore.mu.Unlock()
	kvStore.store[key] = value
}

// Put hash returns the prevValue
func (kvStore *KVStore) PutHash(key string, value string) string {
	// Get prev value
	kvStore.mu.RLock()
	prevValue, exists := kvStore.store[key]
	kvStore.mu.RUnlock()

	if !exists {
		prevValue = ""
	}

	kvStore.mu.Lock()
	kvStore.store[key] = strconv.Itoa(int(hash(prevValue + value)))
	kvStore.mu.Unlock()

	return prevValue
}

func (kvStore *KVStore) Get(key string) (string, bool) {
	kvStore.mu.RLock()
	defer kvStore.mu.RUnlock()

	value, exists := kvStore.store[key]

	return value, exists
}

func (kvStore *KVStore) Copy() *map[string]string {
	kvStore.mu.RLock()
	defer kvStore.mu.RUnlock()

	// Make a copy
	copy := make(map[string]string, len(kvStore.store))
	for k, v := range kvStore.store {
		copy[k] = v
	}

	return &copy
}

type ReqCache struct {
	cache map[uint32]PutReply
	mu    sync.RWMutex
}

func NewReqCache() *ReqCache {
	return &ReqCache{
		cache: make(map[uint32]PutReply),
	}
}

func (reqCache *ReqCache) GetRequestId(clientId string, requestId int64) uint32 {
	return hash(clientId + strconv.FormatInt(requestId, 10))
}

func (reqCache *ReqCache) GetRequest(clientId string, requestId int64) (PutReply, bool) {
	// Get hashId
	hashedId := reqCache.GetRequestId(clientId, requestId)

	// Check if it exists
	reqCache.mu.RLock()
	defer reqCache.mu.RUnlock()
	reply, exists := reqCache.cache[hashedId]

	return reply, exists
}

func (reqCache *ReqCache) WriteRequest(clientId string, requestId int64, reply PutReply) {
	hashedId := reqCache.GetRequestId(clientId, requestId)

	reqCache.mu.Lock()
	reqCache.cache[hashedId] = reply
	reqCache.mu.Unlock()
}

type KVServer struct {
	l           net.Listener
	dead        bool // for testing
	unreliable  bool // for testing
	id          string
	monitorClnt *sysmonitor.Client
	view        sysmonitor.View
	done        sync.WaitGroup
	finish      chan interface{}

	// Add your declarations here.
	kvStore  KVStore
	reqCache ReqCache
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	return nil
}

// ping the view server periodically.
func (server *KVServer) tick() {

	// This line will give an error initially as view and err are not used.
	view, err := server.monitorClnt.Ping(server.view.Viewnum)

	// Your code here.

}

// tell the server to shut itself down.
// please do not change this function.
func (server *KVServer) Kill() {
	server.dead = true
	server.l.Close()
}

func StartKVServer(monitorServer string, id string) *KVServer {
	server := new(KVServer)
	server.id = id
	server.monitorClnt = sysmonitor.MakeClient(id, monitorServer)
	server.view = sysmonitor.View{}
	server.finish = make(chan interface{})

	// Add your server initializations here
	// ==================================
	server.kvStore = *NewKVStore()
	server.reqCache = *NewReqCache()
	//====================================

	rpcs := rpc.NewServer()
	rpcs.Register(server)

	os.Remove(server.id)
	l, e := net.Listen("unix", server.id)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	server.l = l

	// please do not make any changes in the following code,
	// or do anything to subvert it.

	go func() {
		for server.dead == false {
			conn, err := server.l.Accept()
			if err == nil && server.dead == false {
				if server.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if server.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				} else {
					server.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						server.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && server.dead == false {
				fmt.Printf("KVServer(%v) accept: %v\n", id, err.Error())
				server.Kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", server.id)
		server.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(server.finish)
	}()

	server.done.Add(1)
	go func() {
		for server.dead == false {
			server.tick()
			time.Sleep(sysmonitor.PingInterval)
		}
		server.done.Done()
	}()

	return server
}

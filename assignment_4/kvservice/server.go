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
	kvStore.mu.Lock()
	defer kvStore.mu.Unlock()

	prevValue, exists := kvStore.store[key]
	if !exists {
		prevValue = ""
	}

	kvStore.store[key] = strconv.Itoa(int(hash(prevValue + value)))

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

func (reqCache *ReqCache) WriteRequest(clientId string, requestId int64, reply *PutReply) {
	hashedId := reqCache.GetRequestId(clientId, requestId)

	reqCache.mu.Lock()
	reqCache.cache[hashedId] = *reply
	reqCache.mu.Unlock()
}

func (reqCache *ReqCache) Copy() *map[uint32]PutReply {
	reqCache.mu.RLock()
	defer reqCache.mu.RUnlock()

	// Make a copy
	copy := make(map[uint32]PutReply, len(reqCache.cache))
	for k, v := range reqCache.cache {
		copy[k] = v
	}

	return &copy
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

	// Server state
	isPrimary    bool
	isIsolated   bool // For network failures (can't contact sysmonitor)
	latestBackup string

	// Concurrency control
	viewMu sync.RWMutex
	syncMu sync.RWMutex
}

func (server *KVServer) CopyState() (*map[string]string, *map[uint32]PutReply) {
	return server.kvStore.Copy(), server.reqCache.Copy()
}

func (server *KVServer) AcceptState(store *map[string]string, cache *map[uint32]PutReply) {
	server.kvStore.mu.Lock()
	server.reqCache.mu.Lock()
	defer server.kvStore.mu.Unlock()
	defer server.reqCache.mu.Unlock()

	server.kvStore.store = *store
	server.reqCache.cache = *cache
}

func (server *KVServer) InitiateSync(backupAddress string) {
	server.syncMu.Lock()
	store, cache := server.CopyState()
	args := SyncArgs{
		Store: *store,
		Cache: *cache,
	}
	reply := SyncReply{}
	for range MAX_RETRIES {
		DPrintf("Server %v: Initiating state transfer to server %v\n", server.id, backupAddress)
		success := call(backupAddress, "KVServer.SyncState", &args, &reply)
		if success && reply.Err == OK {
			DPrintf("Server %v: State transfer to server %v succeeded\n", server.id, backupAddress)
			break
		}
		time.Sleep(sysmonitor.PingInterval)
	}
	server.syncMu.Unlock()
}

func (server *KVServer) Put(args *PutArgs, reply *PutReply) error {
	server.syncMu.RLock()
	defer server.syncMu.RUnlock()

	DPrintf("Server %v: Put[%v]=%v start with id %v\n", server.id, args.Key, args.Value, server.reqCache.GetRequestId(args.ClientId, args.RequestId))

	server.viewMu.RLock()

	// If server is isolated don't serve
	if server.isIsolated {
		DPrintf("Server %v: Reject request, can't contact sysmonitor\n", server.id)
		reply.Err = ErrWrongServer
		server.viewMu.RUnlock()
		return nil
	}

	// If not primary refuse
	if !server.isPrimary && !args.IsBackup {
		DPrintf("Server %v: Reject request, backup doesn't serve clients\n", server.id)
		reply.Err = ErrWrongServer
		server.viewMu.RUnlock()
		return nil
	}
	server.viewMu.RUnlock()

	// Check for dupes
	if cachedReply, exists := server.reqCache.GetRequest(args.ClientId, args.RequestId); exists {
		DPrintf("Server %v: Duplicate request Put[%v]=%v serve from cache\n", server.id, args.Key, args.Value)
		*reply = cachedReply
		return nil
	}

	// Forward request to backup server
	server.viewMu.RLock()

	if server.isPrimary && server.view.Backup != "" {
		backupArgs := *args
		backupArgs.IsBackup = true
		backupReply := PutReply{}
		for range MAX_RETRIES {
			DPrintf("Server %v: Forwarding Put[%v]=%v to backup server %v\n", server.id, args.Key, args.Value, server.view.Backup)
			success := call(server.view.Backup, "KVServer.Put", &backupArgs, &backupReply)
			if success && backupReply.Err == OK {
				break
			}
			time.Sleep(sysmonitor.PingInterval)
		}
	}
	server.viewMu.RUnlock()

	if args.DoHash {
		reply.PreviousValue = server.kvStore.PutHash(args.Key, args.Value)
	} else {
		server.kvStore.Put(args.Key, args.Value)
	}
	reply.Err = OK

	// Cache request
	server.reqCache.WriteRequest(args.ClientId, args.RequestId, reply)

	DPrintf("Server %v: Put[%v]=%v succeeded\n", server.id, args.Key, args.Value)
	return nil
}

func (server *KVServer) Get(args *GetArgs, reply *GetReply) error {
	server.syncMu.RLock()
	defer server.syncMu.RUnlock()

	DPrintf("Server %v: Get[%v] start\n", server.id, args.Key)

	server.viewMu.RLock()
	// If server is isolated don't serve
	if server.isIsolated {
		DPrintf("Server %v: Reject request, can't contact sysmonitor\n", server.id)
		reply.Err = ErrWrongServer
		server.viewMu.RUnlock()
		return nil
	}

	// If not primary refuse
	if !server.isPrimary && !args.IsBackup {
		DPrintf("Server %v: Reject request, backup doesn't serve clients\n", server.id)
		reply.Err = ErrWrongServer
		server.viewMu.RUnlock()
		return nil
	}

	server.viewMu.RUnlock()

	value, exists := server.kvStore.Get(args.Key)
	reply.Value = value
	reply.Err = OK
	if !exists {
		reply.Err = ErrNoKey
	}

	// Forward request to backup server
	server.viewMu.RLock()
	if server.isPrimary && server.view.Backup != "" {
		backupArgs := *args
		backupArgs.IsBackup = true
		backupReply := GetReply{}
		for range MAX_RETRIES {
			DPrintf("Server %v: Forwarding Get[%v] to backup server %v\n", server.id, args.Key, server.view.Backup)
			success := call(server.view.Backup, "KVServer.Get", &backupArgs, &backupReply)
			if success && (backupReply.Err == OK || backupReply.Err == ErrNoKey) {
				if reply.Value != backupReply.Value {
					DPrintf("Server %v: Inconsistent state with backup server %v\n", server.id, server.view.Backup)
					server.syncMu.RUnlock()
					server.InitiateSync(server.view.Backup)
					server.syncMu.RLock()
				}
				break
			}
			time.Sleep(sysmonitor.PingInterval)
		}
	}
	server.viewMu.RUnlock()

	DPrintf("Server %v: Get[%v]=%v succeeded\n", server.id, args.Key, reply.Value)
	return nil
}

// This RPC is sent to the primary to request its current state
func (server *KVServer) SyncState(args *SyncArgs, reply *SyncReply) error {
	// Wait for all current requests to finish
	server.syncMu.Lock()
	defer server.syncMu.Unlock()

	DPrintf("Server %v: Start state transfer\n", server.id)

	server.AcceptState(&args.Store, &args.Cache)
	reply.Err = OK

	DPrintf("Server %v: State transfer succeeded\n", server.id)
	return nil
}

// ping the view server periodically.
func (server *KVServer) tick() {
	view, err := server.monitorClnt.Ping(server.view.Viewnum)

	server.viewMu.Lock()

	if err != nil {
		DPrintf("Server %v: Error pinging monitor server: %v\n", server.id, err)
		server.isIsolated = true
		server.viewMu.Unlock()
		return
	}
	server.view = view
	server.viewMu.Unlock()

	server.viewMu.RLock()

	// What warrants a state transfer
	needSync := server.view.Backup != "" && server.latestBackup != server.view.Backup

	if server.id == server.view.Primary && needSync {
		// Ask for state transfer
		server.InitiateSync(server.view.Backup)
	}

	server.viewMu.RUnlock()

	server.viewMu.Lock()

	if !server.isPrimary && (server.id == server.view.Primary) {
		DPrintf("Server %v is now primary\nServer %v is now backup\n", server.id, server.view.Backup)
		server.isPrimary = true
	}
	server.latestBackup = server.view.Backup
	server.isIsolated = false
	server.viewMu.Unlock()
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

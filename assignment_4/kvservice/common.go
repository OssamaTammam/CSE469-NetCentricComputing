package kvservice

import (
	"crypto/rand"
	"hash/fnv"
	"math/big"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	MAX_RETRIES    = 2
)

type Err string

type PutArgs struct {
	Key       string
	Value     string
	DoHash    bool // For PutHash
	ClientId  string
	RequestId int64
	IsBackup  bool
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key       string
	ClientId  string
	RequestId int64
	IsBackup  bool
}

type GetReply struct {
	Err   Err
	Value string
}

// Add your RPC definitions here.
// ======================================
type SyncArgs struct {
	Store map[string]string
	Cache map[uint32]PutReply
}

type SyncReply struct {
	Err Err
}

// ======================================

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

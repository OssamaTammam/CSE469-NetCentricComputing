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
	MAX_RETRIES    = 5
)

type Err string

type PutArgs struct {
	Key       string
	Value     string
	DoHash    bool // For PutHash
	ClientId  string
	RequestId int64
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key       string
	ClientId  string
	RequestId int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Add your RPC definitions here.
//======================================

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

package ipldsch

import (
	"github.com/ipfs/go-cid"
)

type Message struct {
	Entries []Entry
	Next    *cid.Cid
}

type Entry struct {
	Block             cid.Cid
	StartIndexInBlock *int
	Length            int
	StartIndexInMsg   uint64
	MDIV              []byte
}
type Send struct {
	Multihash       []byte
	ResumeFromEntry *cid.Cid
}

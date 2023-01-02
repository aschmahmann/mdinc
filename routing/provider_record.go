package routing

import (
	"bytes"
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
)

type NamedProviderRecord struct {
	Name   string
	Record interface{}
}

const sha256ProofName = "/ipld/sha256-md-proof"

func CreateSHA256ProofProviderRecord(c cid.Cid) ([]byte, error) {
	n := bindnode.Wrap(&NamedProviderRecord{Name: sha256ProofName, Record: &c}, nil)
	var buf bytes.Buffer
	if err := dagcbor.Encode(n, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func GetSHA256ProofFromProviderRecord(record []byte) (cid.Cid, error) {
	p := bindnode.Prototype((*NamedProviderRecord)(nil), nil)
	nb := p.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(record)); err != nil {
		return cid.Undef, err
	}
	n, ok := bindnode.Unwrap(nb.Build()).(*NamedProviderRecord)
	if !ok {
		return cid.Undef, fmt.Errorf("was not a named provider record")
	}
	if n.Name != sha256ProofName {
		return cid.Undef, fmt.Errorf("name was %q, not %q", n.Name, sha256ProofName)
	}

	c, ok := n.Record.(*cid.Cid)
	if !ok {
		return cid.Undef, fmt.Errorf("record value was not a cid")
	}
	return *c, nil
}

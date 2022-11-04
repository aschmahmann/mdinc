package mdinc

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/aschmahmann/mdinc/ipldsch"
)

var BlockSize = 1 << 20 // 1 MiB
var NumEntriesPerMsg = 8192

func OutputFile(outputPath string, input io.Reader, withRoot bool, asCarv1 bool) (multihash.Multihash, ipld.Link, error) {
	var roots []cid.Cid
	if withRoot {
		mh, err := multihash.Sum(nil, uint64(multihash.SHA2_256), -1)
		if err != nil {
			return nil, nil, err
		}
		proxyRoot := cid.NewCidV1(uint64(multicodec.DagCbor), mh)
		roots = append(roots, proxyRoot)
	}
	rw, err := carbs.OpenReadWrite(outputPath, roots, carbs.WriteAsCarV1(asCarv1))
	if err != nil {
		return nil, nil, err
	}

	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageWriteOpener = func(ctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		w := new(bytes.Buffer)
		comm := func(link datamodel.Link) error {
			cl := link.(cidlink.Link)
			blk, err := blocks.NewBlockWithCid(w.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			return rw.Put(ctx.Ctx, blk)
		}
		return w, comm, nil
	}

	root, filehash, err := ingest(multicodec.Sha2_256, input, lsys)
	if err != nil {
		return nil, nil, err
	}

	if err := rw.Finalize(); err != nil {
		return nil, nil, err
	}

	if withRoot {
		// re-open/finalize with the final root.
		if err := car.ReplaceRootsInFile(outputPath, []cid.Cid{root.(cidlink.Link).Cid}); err != nil {
			return nil, nil, err
		}
	}

	return filehash, root, nil
}

func ingest(hashcode multicodec.Code, reader io.Reader, lsys ipld.LinkSystem) (ipld.Link, multihash.Multihash, error) {
	switch hashcode {
	case multicodec.Sha2_256:
	default:
		return nil, nil, fmt.Errorf("hashcode not supported %v", hashcode)
	}

	buf := make([]byte, BlockSize)
	var err error
	hasher := sha256.New()
	hashmarshaller := hasher.(encoding.BinaryMarshaler)
	var nextIV []byte
	var processedLen uint64
	var nextLink ipld.Link
	var msgEntries []ipldsch.Entry

	for {
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}

		if nextIV == nil {
			nextIV = GetSHA256InitialIV()
		} else {
			hashbytes, err := hashmarshaller.MarshalBinary()
			if err != nil {
				return nil, nil, err
			}
			nextIV = GetSHA256IVFromMarshalledOutput(hashbytes)
		}
		hasher.Write(buf[:n])

		dataLink, err := lsys.Store(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: intermediateBlockCreator}, basicnode.NewBytes(buf))
		if err != nil {
			return nil, nil, err
		}

		entry, err := createEntry(dataLink, n, nextIV, processedLen)
		if err != nil {
			return nil, nil, err
		}
		if len(msgEntries) < NumEntriesPerMsg {
			msgEntries = append(msgEntries, entry)
		} else {
			l, err := createMessage(lsys, msgEntries, nextLink)
			if err != nil {
				return nil, nil, err
			}
			nextLink = l
			msgEntries = append([]ipldsch.Entry{}, entry)
		}
		processedLen += uint64(n)
	}

	if len(msgEntries) > 0 {
		l, err := createMessage(lsys, msgEntries, nextLink)
		if err != nil {
			return nil, nil, err
		}
		nextLink = l
	}

	digest := hasher.Sum(nil)
	finalMH, err := multihash.Encode(digest, uint64(multicodec.Sha2_256))
	if err != nil {
		return nil, nil, err
	}

	return nextLink, finalMH, nil
}

var intermediateBlockCreator = cid.Prefix{
	Version:  1,
	Codec:    uint64(multicodec.Raw),
	MhType:   uint64(multicodec.Sha2_256),
	MhLength: -1,
}

func GetSHA256IVFromMarshalledOutput(marshalledBytes []byte) []byte {
	magicLen := len("sha\x03")
	return marshalledBytes[magicLen : magicLen+4*8]
}

func GetSHA256InitialIV() []byte {
	init0 := 0x6A09E667
	init1 := 0xBB67AE85
	init2 := 0x3C6EF372
	init3 := 0xA54FF53A
	init4 := 0x510E527F
	init5 := 0x9B05688C
	init6 := 0x1F83D9AB
	init7 := 0x5BE0CD19

	iv := make([]byte, 0, 4*8)
	iv = appendUint32(iv, uint32(init0))
	iv = appendUint32(iv, uint32(init1))
	iv = appendUint32(iv, uint32(init2))
	iv = appendUint32(iv, uint32(init3))
	iv = appendUint32(iv, uint32(init4))
	iv = appendUint32(iv, uint32(init5))
	iv = appendUint32(iv, uint32(init6))
	iv = appendUint32(iv, uint32(init7))

	return iv
}

func appendUint32(b []byte, x uint32) []byte {
	var a [4]byte
	binary.BigEndian.PutUint32(a[:], x)
	return append(b, a[:]...)
}

func createEntry(dataLink ipld.Link, dataLen int, mdiv []byte, startIndexInMsg uint64) (ipldsch.Entry, error) {
	e := ipldsch.Entry{
		Block:             dataLink.(cidlink.Link).Cid,
		StartIndexInBlock: new(int),
		Length:            dataLen,
		StartIndexInMsg:   startIndexInMsg,
		MDIV:              mdiv,
	}
	return e, nil
}

func createMessage(lsys ipld.LinkSystem, entries []ipldsch.Entry, nextMsg ipld.Link) (ipld.Link, error) {
	var nextLnk *cid.Cid
	if nextMsg != nil {
		c := nextMsg.(cidlink.Link).Cid
		nextLnk = &c
	}

	reverseEntries := make([]ipldsch.Entry, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		reverseEntries = append(reverseEntries, entries[i])
	}

	msg := &ipldsch.Message{
		Entries: reverseEntries,
		Next:    nextLnk,
	}

	be := bindnode.Wrap(msg, ipldsch.Prototypes.Message.Type()).Representation()

	lnk, err := lsys.Store(ipld.LinkContext{}, cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagCbor),
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: -1,
		},
	}, be)
	if err != nil {
		return nil, err
	}

	return lnk, nil
}

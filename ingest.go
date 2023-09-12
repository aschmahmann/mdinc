package mdinc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
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

/*
Outboard
Combined
*/

/*
Outboard Spec1:
- CBOR object with:
  - version
  - root CID (optional)
  - inner block size
  - total block size
- Repeated:
  - IV
  - Note: IV isn't enough because it's impossible to know if the outboard is a lie or the data is and they may come from decoupled sources
  - Multihash

Outboard DAG Spec:
- CBOR object with:
  - version
  - root CID
*/

/*
Scenarios:
1. Outboard known/trusted by user (i.e. .torrent files)
  - Only IVs needed
2. Outboard coupled with file (e.g. advertised together in IPNI)
  - Only IVs needed, since if it doesn't work find a new outboard + file to continue from
3. Outboards untrusted and files decoupled
4. Untrusted combined
  - If we insist IV spacing is fixed at X we should be able to easily resume from one to another
  - Side note: fully combined doesn't allow for parallel downloads... but do we care here?
*/

// CreateOutboard2 is a CAR file of CreateDAG with the file blocks stripped out and written as a CARv1
func CreateOutboard2(ctx context.Context, hashcode multicodec.Code, output io.Writer, input io.Reader, inputSize int64) (multihash.Multihash, error) {
	// TODO: We should be able to preallocate the file size and use WriterAt to write the file backwards instead of
	// creating the file in memory and the reversing it

	lsys := cidlink.DefaultLinkSystem()
	var blockList []blocks.Block

	lsys.StorageWriteOpener = func(context linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			cl, ok := l.(cidlink.Link)
			if !ok {
				return fmt.Errorf("not a cidlink")
			}

			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}

			blockList = append(blockList, blk)
			return nil
		}, nil
	}

	dagRoot, largeBlockHash, err := CreateDAG2(hashcode, input, &lsys, false)
	if err != nil {
		return nil, err
	}

	wc, err := storage.NewWritable(output, []cid.Cid{dagRoot.(cidlink.Link).Cid}, car.WriteAsCarV1(true))
	if err != nil {
		return nil, err
	}

	for i := len(blockList) - 1; i >= 0; i-- {
		blk := blockList[i]
		if err := wc.Put(ctx, blk.Cid().KeyString(), blk.RawData()); err != nil {
			return nil, err
		}
	}

	return largeBlockHash, nil
}

func CreateOutboard(hashcode multicodec.Code, output io.WriterAt, input io.Reader, inputSize int64) (multihash.Multihash, error) {
	if hashcode != multicodec.Sha2_256 {
		return nil, fmt.Errorf("only SHA2_256 is supported")
	}

	// Compute metadata size
	blankMh, err := multihash.Sum(nil, uint64(hashcode), -1)
	if err != nil {
		return nil, fmt.Errorf("could not create placeholder multihash %w", err)
	}

	md := ipldsch.OutboardMetadata{
		Version:   1,
		Root:      cid.NewCidV1(cid.Raw, blankMh),
		ChunkSize: uint(BlockSize),
		TotalSize: uint64(inputSize),
	}
	be := bindnode.Wrap(&md, ipldsch.Prototypes.Message.Type()).Representation()
	var metadataBuf bytes.Buffer
	if err := dagcbor.Encode(be, &metadataBuf); err != nil {
		return nil, err
	}
	metadataSize := int64(metadataBuf.Len())

	const ivSize = 32
	mhSize := int64(len(blankMh))
	entrySize := ivSize + mhSize

	numChunks := inputSize / int64(BlockSize)
	if numChunks*int64(BlockSize) != inputSize {
		numChunks++
	}
	totalSize := metadataSize + numChunks*entrySize

	// Write all of the IVs backwards so that the last IV is at the beginning
	numIVsWritten := int64(0)
	inputMh, err := fixedMdChunker(hashcode, input, func(iv, chunk []byte) error {
		numIVsWritten++
		offset := totalSize - entrySize*numIVsWritten
		if _, err := output.WriteAt(iv, offset); err != nil {
			return err
		}

		chunkMh, err := multihash.Sum(chunk, uint64(hashcode), -1)
		if err != nil {
			return err
		}

		if _, err := output.WriteAt(chunkMh, offset); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Now that we know the final hash, write the metadata
	md.Root = cid.NewCidV1(cid.Raw, inputMh)
	be = bindnode.Wrap(md, ipldsch.Prototypes.Message.Type()).Representation()
	metadataBuf.Reset()
	if err := dagcbor.Encode(be, &metadataBuf); err != nil {
		return nil, err
	}
	if _, err := output.WriteAt(metadataBuf.Bytes(), 0); err != nil {
		return nil, err
	}

	return inputMh, nil
}

func CreateCombined(hashcode multicodec.Code, output io.WriterAt, input io.Reader, inputSize int64) (multihash.Multihash, error) {
	// Compute metadata size
	blankMh, err := multihash.Sum(nil, uint64(hashcode), -1)
	if err != nil {
		return nil, fmt.Errorf("could not create placeholder multihash %w", err)
	}

	md := ipldsch.OutboardMetadata{
		Version:   1,
		Root:      cid.NewCidV1(cid.Raw, blankMh),
		ChunkSize: uint(BlockSize),
		TotalSize: uint64(inputSize),
	}
	be := bindnode.Wrap(&md, ipldsch.Prototypes.Message.Type()).Representation()
	var metadataBuf bytes.Buffer
	if err := dagcbor.Encode(be, &metadataBuf); err != nil {
		return nil, err
	}
	metadataSize := int64(metadataBuf.Len())

	const ivSize = 32
	numChunks := inputSize / int64(BlockSize)
	if numChunks*int64(BlockSize) != inputSize {
		numChunks++
	}
	totalSize := metadataSize + numChunks*ivSize

	// Write all of the IVs backwards so that the last IV is at the beginning
	numIVsWritten := 0
	inputMh, err := fixedMdChunker(hashcode, input, func(iv, chunk []byte) error {
		numIVsWritten++
		offset := totalSize - int64(ivSize*numIVsWritten)
		if _, err := output.WriteAt(iv, offset); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Now that we know the final hash, write the metadata
	md.Root = cid.NewCidV1(cid.Raw, inputMh)
	be = bindnode.Wrap(md, ipldsch.Prototypes.Message.Type()).Representation()
	metadataBuf.Reset()
	if err := dagcbor.Encode(be, &metadataBuf); err != nil {
		return nil, err
	}
	if _, err := output.WriteAt(metadataBuf.Bytes(), 0); err != nil {
		return nil, err
	}

	return inputMh, nil
}

func CreateDAG(hashcode multicodec.Code, input io.Reader, lsys *linking.LinkSystem) (ipld.Link, multihash.Multihash, error) {
	return CreateDAG2(hashcode, input, lsys, true)
}

func CreateDAG2(hashcode multicodec.Code, input io.Reader, lsys *linking.LinkSystem, storeLeafBlocks bool) (ipld.Link, multihash.Multihash, error) {
	processedLen := uint64(0)
	var msgEntries []ipldsch.Entry
	var nextLink ipld.Link

	inputMh, err := fixedMdChunker(hashcode, input, func(iv, chunk []byte) error {
		var dataLink datamodel.Link
		var err error
		if storeLeafBlocks {
			dataLink, err = lsys.Store(ipld.LinkContext{}, cidlink.LinkPrototype{Prefix: intermediateBlockCreator}, basicnode.NewBytes(chunk))
			if err != nil {
				return err
			}
		} else {
			c, err := cidlink.LinkPrototype{Prefix: intermediateBlockCreator}.Sum(chunk)
			if err != nil {
				return err
			}
			dataLink = cidlink.Link{Cid: c}
		}

		n := len(chunk)
		entry, err := createEntry(dataLink, n, iv, processedLen)
		if err != nil {
			return err
		}
		if len(msgEntries) < NumEntriesPerMsg {
			msgEntries = append(msgEntries, entry)
		} else {
			l, err := createMessage(*lsys, msgEntries, nextLink)
			if err != nil {
				return err
			}
			nextLink = l
			msgEntries = append([]ipldsch.Entry{}, entry)
		}
		processedLen += uint64(n)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	if len(msgEntries) > 0 {
		l, err := createMessage(*lsys, msgEntries, nextLink)
		if err != nil {
			return nil, nil, err
		}
		nextLink = l
	}

	return nextLink, inputMh, nil
}

func fixedMdChunker(hashcode multicodec.Code, input io.Reader, output func(iv, chunk []byte) error) (multihash.Multihash, error) {
	switch hashcode {
	case multicodec.Sha2_256:
	default:
		return nil, fmt.Errorf("hashcode not supported %v", hashcode)
	}

	buf := make([]byte, BlockSize)
	var err error
	hasher := sha256.New()
	hashmarshaller := hasher.(encoding.BinaryMarshaler)
	var nextIV []byte

	for {
		n, err := input.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if nextIV == nil {
			nextIV = GetSHA256InitialIV()
		} else {
			hashbytes, err := hashmarshaller.MarshalBinary()
			if err != nil {
				return nil, err
			}
			nextIV = GetSHA256IVFromMarshalledOutput(hashbytes)
		}
		hasher.Write(buf[:n])

		if err := output(nextIV, buf); err != nil {
			return nil, err
		}
	}

	digest := hasher.Sum(nil)
	finalMH, err := multihash.Encode(digest, uint64(multicodec.Sha2_256))
	if err != nil {
		return nil, err
	}

	return finalMH, nil
}

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

	root, filehash, err := CreateDAG(multicodec.Sha2_256, input, &lsys)
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

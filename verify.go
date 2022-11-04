package mdinc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/ipfs/go-bitswap/client/sessioniface"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/aschmahmann/mdinc/ipldsch"
)

var logger = log.Logger("mdinc/verify")

// Verify validates that the proof matches the given multihash.
// Implementation note: It walks the proof linearly which minimizes exposure to fraudulent proofs.
func Verify(mh multihash.Multihash, proof cid.Cid, lsys ipld.LinkSystem) error {
	dmh, err := multihash.Decode(mh)
	if err != nil {
		return err
	}
	if dmh.Code != uint64(multicodec.Sha2_256) || dmh.Length != 32 {
		return fmt.Errorf("only SHA2-256-256 supported not %v", dmh)
	}

	hasher := sha256.New().(marshallableHasher)
	expectedDigest := dmh.Digest
	isFirstMessageEntry := true

	var latestMessage *ipldsch.Message
	var nextProofLink *cid.Cid = &proof

	for nextProofLink != nil {
		nd, err := lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: *nextProofLink}, ipldsch.Prototypes.Message)
		if err != nil {
			return err
		}

		bnd := bindnode.Unwrap(nd)
		latestMessage = bnd.(*ipldsch.Message)

		for i, e := range latestMessage.Entries {
			bl := e.Block
			if bl.Type() != uint64(multicodec.Raw) {
				panic("blocks must be raw")
			}
			blockData, err := lsys.LoadRaw(ipld.LinkContext{}, cidlink.Link{Cid: bl})
			if err != nil {
				return err
			}

			startIndexInBlock := 0
			if e.StartIndexInBlock != nil {
				startIndexInBlock = *e.StartIndexInBlock
			}

			if err := decodeSHA256Hasher(hasher, e.MDIV, e.StartIndexInMsg); err != nil {
				return err
			}

			if _, err := hasher.Write(blockData[startIndexInBlock : startIndexInBlock+e.Length]); err != nil {
				return err
			}

			if i == 0 && isFirstMessageEntry {
				calculatedDigest := hasher.Sum(nil)
				if !bytes.Equal(expectedDigest, calculatedDigest) {
					return fmt.Errorf("expected digest %x, got %x", expectedDigest, calculatedDigest)
				}
				expectedDigest = e.MDIV
				isFirstMessageEntry = false
			} else {
				out, err := hasher.MarshalBinary()
				if err != nil {
					return err
				}
				calculatedPartialDigest := GetSHA256IVFromMarshalledOutput(out)
				if !bytes.Equal(expectedDigest, calculatedPartialDigest) {
					return fmt.Errorf("expected partial digest %x, got %x", expectedDigest, calculatedPartialDigest)
				}
				expectedDigest = e.MDIV
			}
		}

		nextProofLink = latestMessage.Next
	}

	earliestEntry := latestMessage.Entries[len(latestMessage.Entries)-1]
	if initDigest, msgInitDigest := GetSHA256InitialIV(), earliestEntry.MDIV; !bytes.Equal(initDigest, msgInitDigest) {
		return fmt.Errorf("expected initial digest %x, got %x", expectedDigest, msgInitDigest)
	}
	if earliestEntry.StartIndexInMsg != 0 {
		return fmt.Errorf("expected earliest entry to start at the beginning of the message instead it started at %d bytes in", earliestEntry.StartIndexInMsg)
	}

	return nil
}

type Loader interface {
	GetBlock(context.Context, cid.Cid) (blocks.Block, error)
	GetBlocksCh(ctx context.Context, keys <-chan sessioniface.AddRemoveCid) (<-chan blocks.Block, error)
}

type BlockLoader struct {
	lsys     ipld.LinkSystem
	nworkers int
}

func NewBlockLoader(lsys ipld.LinkSystem, nworkers int) *BlockLoader {
	return &BlockLoader{lsys: lsys, nworkers: nworkers}
}

func NewBlockLoaderFromBlockstore(bs blockstore.Blockstore, nworkers int) *BlockLoader {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(ctx linking.LinkContext, link datamodel.Link) (io.Reader, error) {
		cl := link.(cidlink.Link)
		blk, err := bs.Get(ctx.Ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}

	return &BlockLoader{lsys: lsys, nworkers: nworkers}
}

func (b *BlockLoader) GetBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	data, err := b.lsys.LoadRaw(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return nil, err
	}
	blk, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func (b *BlockLoader) GetBlocksCh(ctx context.Context, keys <-chan sessioniface.AddRemoveCid) (<-chan blocks.Block, error) {
	eg, gctx := errgroup.WithContext(ctx)
	receivedBlockCh := make(chan blocks.Block)
	for i := 0; i < b.nworkers; i++ {
		eg.Go(func() error {
			for addrmCid := range keys {
				c := addrmCid.Key()
				if !addrmCid.IsAdd() {
					panic("only key addition is supported")
				}
				blkData, err := b.lsys.LoadRaw(ipld.LinkContext{}, cidlink.Link{Cid: c})
				if err != nil {
					return err
				}
				blk, err := blocks.NewBlockWithCid(blkData, c)
				if err != nil {
					return err
				}
				select {
				case receivedBlockCh <- blk:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			return nil
		})
	}
	return receivedBlockCh, nil
}

var _ Loader = (*BlockLoader)(nil)

// SpeedyVerify validates that the proof matches the given multihash and returns the data.
// Implementation note: It geometrically grows trust in the proof as good blocks are returned
func SpeedyVerify(ctx context.Context, mh multihash.Multihash, proof cid.Cid, lsys Loader, unverifiedBS blockstore.Blockstore) ([]byte, error) {
	dmh, err := multihash.Decode(mh)
	if err != nil {
		return nil, err
	}
	if dmh.Code != uint64(multicodec.Sha2_256) || dmh.Length != 32 {
		return nil, fmt.Errorf("only SHA2-256-256 supported not %v", dmh)
	}

	var nextProofLink *cid.Cid = &proof
	data := &bytes.Buffer{}

	loadMsg := func(c cid.Cid) (*ipldsch.Message, error) {
		blk, err := lsys.GetBlock(ctx, c)
		if err != nil {
			return nil, err
		}
		blkBytes := blk.RawData()
		nb := ipldsch.Prototypes.Message.NewBuilder()
		if err := dagcbor.Decode(nb, bytes.NewReader(blkBytes)); err != nil {
			return nil, err
		}
		nd := nb.Build()
		bnd := bindnode.Unwrap(nd)
		msg := bnd.(*ipldsch.Message)
		return msg, nil
	}

	const maxEntriesStored = 1 >> 15

	getEntryCh := make(chan ipldsch.Entry)
	addEntryMapCh := make(chan ipldsch.Entry)

	tmpExpectedSize := uint64(0)

	keysCh := make(chan sessioniface.AddRemoveCid)
	receivedBlockCh, err := lsys.GetBlocksCh(ctx, keysCh)
	if err != nil {
		return nil, err
	}

	eg, gctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(getEntryCh)
		for nextProofLink != nil {
			msg, err := loadMsg(*nextProofLink)
			if err != nil {
				return err
			}
			for _, e := range msg.Entries {
				select {
				case getEntryCh <- e:
					logger.Infof("requesting CID %v ending at %v\n", e.Block, e.StartIndexInMsg+uint64(e.Length))
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			nextProofLink = msg.Next
		}
		return nil
	})
	eg.Go(func() error {
		defer close(addEntryMapCh)

		for e := range getEntryCh {
			select {
			case addEntryMapCh <- e:
			case <-gctx.Done():
				return gctx.Err()
			}
			select {
			case keysCh <- addRemoveCid{key: e.Block, add: true}:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	eg.Go(func() error {
		send := addEntryMapCh
		rcv := receivedBlockCh

		positionNotSet := true
		validateFromPosition := uint64(0)

		const startOutstandingBlocks = 2
		allowedOutstandingBlocks := startOutstandingBlocks

		hasher := sha256.New().(marshallableHasher)
		expectedDigest := dmh.Digest

		isFirstMessageEntry := true
		type pendingEntry struct {
			endingIndex uint64
			val         ipldsch.Entry
		}
		var pendingEntries []pendingEntry
		var latestVal pendingEntry
		sendDone := false

		processRcv := func(c cid.Cid) (bool, error) {
			if positionNotSet {
				return false, fmt.Errorf("somehow the position hasn't been set and we're receiving blocks")
			}
			if latestVal.val.Block != c {
				return false, nil
			}
			if latestVal.endingIndex != validateFromPosition {
				return false, fmt.Errorf("we can only handle non-overlapping blocks")
			}

			if !sendDone && send == nil {
				send = addEntryMapCh
			}
			allowedOutstandingBlocks += 2

			e := latestVal.val
			startIndexInBlock := 0
			if e.StartIndexInBlock != nil {
				startIndexInBlock = *e.StartIndexInBlock
			}

			if err := decodeSHA256Hasher(hasher, e.MDIV, e.StartIndexInMsg); err != nil {
				return false, err
			}

			blk, err := unverifiedBS.Get(ctx, c)
			if err != nil {
				return false, err
			}
			blockData := blk.RawData()

			usefulBlkData := blockData[startIndexInBlock : startIndexInBlock+e.Length]
			if _, err := hasher.Write(usefulBlkData); err != nil {
				return false, err
			}
			validateFromPosition -= uint64(e.Length)

			if isFirstMessageEntry {
				calculatedDigest := hasher.Sum(nil)
				if !bytes.Equal(expectedDigest, calculatedDigest) {
					return false, fmt.Errorf("expected digest %x, got %x", expectedDigest, calculatedDigest)
				}
				expectedDigest = e.MDIV
				isFirstMessageEntry = false
				tmpExpectedSize = e.StartIndexInMsg + uint64(e.Length)
			} else {
				out, err := hasher.MarshalBinary()
				if err != nil {
					return false, err
				}
				calculatedPartialDigest := GetSHA256IVFromMarshalledOutput(out)
				if !bytes.Equal(expectedDigest, calculatedPartialDigest) {
					return false, fmt.Errorf("expected partial digest %x, got %x", expectedDigest, calculatedPartialDigest)
				}
				expectedDigest = e.MDIV
			}

			inPlaceReverseByteSlice(usefulBlkData)
			if _, err := data.Write(usefulBlkData); err != nil {
				return false, err
			}
			logger.Infof("received CID %s: data verified %2f%% \n", c, float64(len(data.Bytes()))*100/float64(tmpExpectedSize))

			if e.StartIndexInMsg == 0 {
				if initDigest, msgInitDigest := GetSHA256InitialIV(), e.MDIV; !bytes.Equal(initDigest, msgInitDigest) {
					return false, fmt.Errorf("expected initial digest %x, got %x", expectedDigest, msgInitDigest)
				}
				return true, nil
			}

			pendingEntries = pendingEntries[1:]
			if len(pendingEntries) == 0 {
				latestVal = pendingEntry{}
			} else {
				latestVal = pendingEntries[0]
			}
			return false, nil
		}

		for {
			if !latestVal.val.Block.Equals(cid.Undef) {
				hasBlk, err := unverifiedBS.Has(ctx, latestVal.val.Block)
				if err != nil {
					return err
				}
				if hasBlk {
					done, err := processRcv(latestVal.val.Block)
					if err != nil {
						return err
					}
					if done {
						return nil
					}
					continue
				}
			}
			select {
			case s, ok := <-send:
				if !ok {
					sendDone = true
					send = nil
					continue
				}
				sVal := pendingEntry{endingIndex: s.StartIndexInMsg + uint64(s.Length), val: s}
				if positionNotSet {
					positionNotSet = false
					latestVal = sVal
					validateFromPosition = s.StartIndexInMsg + uint64(s.Length)
				} else if latestVal.val.Block.Equals(cid.Undef) {
					latestVal = sVal
				}

				pendingEntries = append(pendingEntries, sVal)
				allowedOutstandingBlocks--
				if len(pendingEntries) == maxEntriesStored || allowedOutstandingBlocks == 0 {
					send = nil
				}
			case blk, ok := <-rcv:
				if !ok {
					return nil
				}
				if err := unverifiedBS.Put(ctx, blk); err != nil {
					return err
				}
				done, err := processRcv(blk.Cid())
				if err != nil {
					return err
				}
				if done {
					return nil
				}
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	b := data.Bytes()
	inPlaceReverseByteSlice(b)
	return b, nil
}

func inPlaceReverseByteSlice(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

type marshallableHasher interface {
	hash.Hash
	encoding.BinaryUnmarshaler
	encoding.BinaryMarshaler
}

func decodeSHA256Hasher(h marshallableHasher, iv []byte, length uint64) error {
	magic256 := "sha\x03"
	chunk := 64
	marshalledBytes := make([]byte, 0, len(magic256)+8*4+chunk+8)
	marshalledBytes = append(marshalledBytes, []byte(magic256)...)
	marshalledBytes = append(marshalledBytes, iv...)
	marshalledBytes = marshalledBytes[:cap(marshalledBytes)]
	binary.BigEndian.PutUint64(marshalledBytes[len(marshalledBytes)-8:], length)
	return h.UnmarshalBinary(marshalledBytes)
}

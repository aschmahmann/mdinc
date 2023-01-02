package mdinc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	delay "github.com/ipfs/go-ipfs-delay"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

func TestRoundtrip(t *testing.T) {
	NumEntriesPerMsg = 3

	t.Run("Size-1MiB+1", func(t *testing.T) { testRoundTripSize(t, (1<<20)+1, false) })
	t.Run("Size-2MiB", func(t *testing.T) { testRoundTripSize(t, 2<<20, false) })
	t.Run("Size-9MiB", func(t *testing.T) { testRoundTripSize(t, 9<<20, false) })
	t.Run("Size-10MiB", func(t *testing.T) { testRoundTripSize(t, 10<<20, false) })
}

func TestSpeedyRoundtrip(t *testing.T) {
	NumEntriesPerMsg = 100
	BlockSize = 100 << 10

	t.Run("Size-1MiB+1", func(t *testing.T) { testRoundTripSize(t, (1<<20)+1, true) })
	t.Run("Size-2MiB", func(t *testing.T) { testRoundTripSize(t, 2<<20, true) })
	t.Run("Size-9MiB", func(t *testing.T) { testRoundTripSize(t, 9<<20, true) })
	t.Run("Size-10MiB", func(t *testing.T) { testRoundTripSize(t, 10<<20, true) })
	t.Run("Size-100MiB", func(t *testing.T) { testRoundTripSize(t, 10<<20*10, true) })
}

func testRoundTripSize(t *testing.T, sz int, speedy bool) {
	t.Parallel()
	datafile, err := ioutil.TempFile("", fmt.Sprintf("%d-*.file", sz))
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(int64(sz)))
	if _, err := io.CopyN(datafile, rng, int64(sz)); err != nil {
		t.Fatal(err)
	}
	outfile, err := ioutil.TempFile("", fmt.Sprintf("%d-*.car", sz))
	if err != nil {
		t.Fatal(err)
	}
	_, err = datafile.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	mh, lnk, err := OutputFile(outfile.Name(), datafile, true, true)
	if err != nil {
		t.Fatal(err)
	}

	bs, err := carbs.OpenReadOnly(outfile.Name())
	if err != nil {
		t.Fatal(err)
	}
	bstore := DelayedGet{Blockstore: bs, delay: delay.Fixed(time.Second)}
	lsys := cidlink.DefaultLinkSystem()
	lk := sync.Mutex{}
	x := 0
	lsys.StorageReadOpener = func(ctx linking.LinkContext, link datamodel.Link) (io.Reader, error) {
		cl := link.(cidlink.Link)
		lk.Lock()
		x++
		t.Logf("open number %d", x)
		lk.Unlock()
		blk, err := bstore.Get(ctx.Ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}

	proof := lnk.(cidlink.Link).Cid
	if speedy {
		bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
		ld := NewBlockLoader(lsys, 100)
		if _, err := SpeedyVerify(context.Background(), mh, proof, ld, bs); err != nil {
			t.Fatal(err)
		}
		return
	}

	if err := Verify(mh, proof, lsys); err != nil {
		t.Fatal(err)
	}
}

// DelayedGet is an adapter that delays Get operations on the inner blockstore.
type DelayedGet struct {
	blockstore.Blockstore
	delay delay.D
}

var _ blockstore.Blockstore = (*DelayedGet)(nil)

func (dds *DelayedGet) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	dds.delay.Wait()
	return dds.Blockstore.Get(ctx, cid)
}

package mdinc

import (
	"bytes"
	"context"
	"io"

	bsclient "github.com/ipfs/go-bitswap/client"
	"github.com/ipfs/go-bitswap/client/sessioniface"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	rhelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/host"
)

type addRemoveCid struct {
	key cid.Cid
	add bool
}

func (a addRemoveCid) IsAdd() bool {
	return a.add
}

func (a addRemoveCid) Key() cid.Cid {
	return a.key
}

var _ sessioniface.AddRemoveCid = (*addRemoveCid)(nil)

func bitswapBlockLoader(ctx context.Context, h host.Host) Loader {
	n := bsnet.NewFromIpfsHost(h, rhelpers.Null{})
	bs := bstore.NewBlockstore(datastore.NewMapDatastore())
	c := bsclient.New(ctx, n, bs)
	n.Start(c)
	s := c.NewSession(ctx)

	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(linkContext linking.LinkContext, link datamodel.Link) (io.Reader, error) {
		blk, err := s.GetBlock(linkContext.Ctx, link.(cidlink.Link).Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(blk.RawData()), nil
	}
	ld := NewBlockLoader(lsys, 100)
	return ld
}

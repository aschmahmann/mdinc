package mdinc

import (
	"context"
	"fmt"
	"github.com/aschmahmann/mdinc/routing"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"

	bsclient "github.com/ipfs/go-bitswap/client"
	"github.com/ipfs/go-bitswap/client/sessioniface"

	drc "github.com/ipfs/go-libipfs/routing/http/client"
	drt "github.com/ipfs/go-libipfs/routing/http/types"
)

const blockLimit = 1 << 21 // 2MiB

var Download2 = DownloadWithDiscovery

func DownloadWithDiscovery(ctx context.Context, mh multihash.Multihash, routerUrl string, h host.Host, downloadClient *bsclient.Client) ([]byte, error) {
	initialLookupCtx, initialLookupCancel := context.WithTimeout(ctx, time.Second*1)
	defer initialLookupCancel()

	drpclient, err := drc.New(routerUrl)
	if err != nil {
		return nil, err
	}

	fprovsresp, err := drpclient.FindProviders(initialLookupCtx, cid.NewCidV1(uint64(multicodec.Raw), mh))
	initialLookupCancel()
	if err != nil && len(fprovsresp) == 0 {
		return nil, err
	}

	var proofs []cid.Cid
	var peers []peer.AddrInfo
	for _, r := range fprovsresp {
		switch pr := r.(type) {
		case *drt.ReadBitswapProviderRecord:
			ai := peer.AddrInfo{
				ID:    *pr.ID,
				Addrs: nil,
			}
			for _, a := range pr.Addrs {
				ai.Addrs = append(ai.Addrs, a.Multiaddr)
			}
			peers = append(peers, ai)
		case *drt.UnknownProviderRecord:
			// TODO: check protocol name, figure out deal with schema name
			c, err := routing.GetSHA256ProofFromProviderRecord(pr.Bytes)
			if err != nil {
				logger.Infof("could not decode provider record: %q", err.Error())
				continue
			}
			proofs = append(proofs, c)
		}
	}

	for _, p := range peers {
		go func() { h.Connect(ctx, p) }()
	}

	rawCid := cid.NewCidV1(uint64(multicodec.Raw), mh)

	var data []byte

	gpCtx, gpCtxCancel := context.WithCancel(ctx)
	defer gpCtxCancel()
	gp := sync.WaitGroup{}
	gp.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer gp.Done()
		blockGetTimeoutCtx, blockGetTimeoutCancel := context.WithTimeout(gpCtx, time.Second*5)
		defer blockGetTimeoutCancel()
		if len(peers) > 0 {
			blk, err := downloadClient.GetBlock(blockGetTimeoutCtx, rawCid)
			if err != nil {
				errCh <- err
				return
			}
			data = blk.RawData()
			gpCtxCancel()
		}
	}()
	go func() {
		defer gp.Done()
		if len(proofs) > 0 {
			// TODO: Proof get timeout
			// TODO: Multiple proofs
			proof := proofs[0]
			bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
			blkDownloader := downloadClient.NewSession(ctx).(sessioniface.ChannelFetcher)
			largeBlock, err := SpeedyVerify(gpCtx, mh, proof, blkDownloader, bs)
			if err != nil {
				errCh <- err
				return
			}
			// TODO: Should fail internally in the verifier
			if len(largeBlock) <= blockLimit {
				errCh <- fmt.Errorf("someone tried to pass off a small block as a large one")
				return
			}
			data = largeBlock
			gpCtxCancel()
		}
	}()
	gp.Wait()
	close(errCh)

	if len(data) > 0 {
		return data, nil
	}

	var retErr error
	for e := range errCh {
		if e != nil {
			retErr = multierror.Append(err, e)
		}
	}

	return nil, fmt.Errorf("could not find data - received errors: %w", retErr)
}

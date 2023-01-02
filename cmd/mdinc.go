package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/aschmahmann/mdinc/routing"
	"io"
	"log"
	"os"

	"github.com/urfave/cli/v2"

	bsclient "github.com/ipfs/go-bitswap/client"
	"github.com/ipfs/go-bitswap/client/sessioniface"
	bsnet "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	drc "github.com/ipfs/go-delegated-routing/client"
	drp "github.com/ipfs/go-delegated-routing/gen/proto"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	carbs "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	rhelpers "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"

	"github.com/aschmahmann/mdinc"
)

func main() {
	app := &cli.App{
		Name:  "mdinc",
		Usage: "tools for working with incrementally verifiable large merkle-damgard blocks",
		Commands: []*cli.Command{
			{
				Name:  "ingest",
				Usage: "<filepath> <outputpath>",
				Description: "Take a file and turn it into a CAR file with both the file data and the proof information required for incremental verifiability. " +
					"Returns the digest along with the CID of the proof. Currently only SHA2-256 is supported.",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "json",
						Usage: "output data as JSON",
						Value: false,
					},
				},
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("invalid number of arguments")
					}
					fpath := ctx.Args().Get(0)
					opath := ctx.Args().Get(1)

					finput, err := os.Open(fpath)
					if err != nil {
						return err
					}

					fileDigest, proofRoot, err := mdinc.OutputFile(opath, finput, true, true)
					if err != nil {
						return err
					}

					fileDigestString, err := multibase.Encode(multibase.Base16, fileDigest)
					if err != nil {
						return err
					}

					if ctx.Bool("json") {
						fmt.Printf("{ \"filehash\" : %q, \"proofCID\" : %q }\n", fileDigestString, proofRoot)
					} else {
						fmt.Printf("filehash %v, proofCID %v\n", fileDigestString, proofRoot)
					}
					return nil
				},
			},
			{
				Name:  "verify",
				Usage: "<multibase-multihash> <car-path> [proofCID]",
				Description: "Takes a multihash and a CAR and attempts to verify that the data with that hash is in the CAR. " +
					"The proof CID must either be the only root in the CAR file or passed explicitly.",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "speedy",
						Value: false,
						Usage: "Enables using the more complex, but faster verification scheme",
					},
				},
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() > 3 || ctx.NArg() < 2 {
						return errors.New("invalid number of arguments")
					}
					mhStr := ctx.Args().Get(0)
					carPath := ctx.Args().Get(1)
					_, mhBytes, err := multibase.Decode(mhStr)
					if err != nil {
						return err
					}
					mh, err := multihash.Cast(mhBytes)
					if err != nil {
						return err
					}

					bs, err := carbs.OpenReadOnly(carPath)
					if err != nil {
						return err
					}

					var proof cid.Cid
					if ctx.NArg() == 3 {
						cidStr := ctx.Args().Get(2)
						proof, err = cid.Decode(cidStr)
						if err != nil {
							return err
						}
					} else {
						roots, err := bs.Roots()
						if err != nil {
							return err
						}
						if len(roots) != 1 {
							return fmt.Errorf("the proof root must be passed unless there is a single root in the CAR file")
						}
						proof = roots[0]
					}

					lsys := cidlink.DefaultLinkSystem()
					lsys.StorageReadOpener = func(ctx linking.LinkContext, link datamodel.Link) (io.Reader, error) {
						cl := link.(cidlink.Link)
						blk, err := bs.Get(ctx.Ctx, cl.Cid)
						if err != nil {
							return nil, err
						}
						return bytes.NewReader(blk.RawData()), nil
					}

					if ctx.Bool("speedy") {
						bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
						ld := mdinc.NewBlockLoader(lsys, 100)
						_, err := mdinc.SpeedyVerify(ctx.Context, mh, proof, ld, bs)
						return err
					}

					return mdinc.Verify(mh, proof, lsys)
				},
			},
			{
				Name:        "download",
				Usage:       "<multibase-multihash> <proofCID> <multiaddr>",
				Description: "Download the data associated with multihash using the given proof CID from a particular multiaddr. The target must speak the Bitswap protocol.",
				Flags: []cli.Flag{
					&cli.PathFlag{
						Name:     "output",
						Usage:    "output file path. if not defined will print to stdout",
						Required: false,
						Aliases:  []string{"o"},
					},
				},
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 3 {
						return errors.New("invalid number of arguments")
					}
					mhStr := ctx.Args().Get(0)
					cidStr := ctx.Args().Get(1)
					maStr := ctx.Args().Get(2)
					_, mhBytes, err := multibase.Decode(mhStr)
					if err != nil {
						return err
					}
					mh, err := multihash.Cast(mhBytes)
					if err != nil {
						return err
					}

					proof, err := cid.Decode(cidStr)
					if err != nil {
						return err
					}

					ai, err := peer.AddrInfoFromString(maStr)
					if err != nil {
						return err
					}
					h, err := libp2p.New(libp2p.DefaultMuxers, libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport))
					if err != nil {
						return err
					}

					n := bsnet.NewFromIpfsHost(h, rhelpers.Null{})
					// Note: this blockstore is largely irrelevant and seems to mostly be for metrics collection
					bsclientBlockstore := blockstore.NewBlockstore(datastore.NewMapDatastore())
					c := bsclient.New(ctx.Context, n, bsclientBlockstore)
					n.Start(c)
					s := c.NewSession(ctx.Context)
					cf, ok := s.(sessioniface.ChannelFetcher)
					if !ok {
						return fmt.Errorf("session not a channel fetcher")
					}
					if err != nil {
						return err
					}
					if err := h.Connect(ctx.Context, *ai); err != nil {
						return err
					}

					bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
					data, err := mdinc.SpeedyVerify(ctx.Context, mh, proof, cf, bs)
					if err != nil {
						return err
					}

					if outputPath := ctx.Path("output"); outputPath != "" {
						return os.WriteFile(outputPath, data, os.ModePerm)
					}
					if _, err := os.Stdout.Write(data); err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:  "download-with-discovery",
				Usage: "<multibase-multihash> <router-url>",
				Description: "Download the data associated with multihash using the given router for discovery/content routing. " +
					"The router must be a Reframe implementation that supports returning proofs for large blocks " +
					"and the targets that have the data must speak the Bitswap protocol.",
				Flags: []cli.Flag{
					&cli.PathFlag{
						Name:     "output",
						Usage:    "output file path. if not defined will print to stdout",
						Required: false,
						Aliases:  []string{"o"},
					},
				},
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("invalid number of arguments")
					}
					mhStr := ctx.Args().Get(0)
					routerUrl := ctx.Args().Get(1)
					_, mhBytes, err := multibase.Decode(mhStr)
					if err != nil {
						return err
					}
					mh, err := multihash.Cast(mhBytes)
					if err != nil {
						return err
					}

					h, err := libp2p.New(libp2p.DefaultMuxers, libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport))
					if err != nil {
						return err
					}

					drpclient, err := drp.New_DelegatedRouting_Client(routerUrl)
					if err != nil {
						return err
					}

					drclient, err := drc.NewClient(drpclient, nil, nil)
					if err != nil {
						return err
					}
					dr := drc.NewContentRoutingClient(drclient)

					n := bsnet.NewFromIpfsHost(h, dr)
					bs := blockstore.NewBlockstore(datastore.NewMapDatastore())
					downloadClient := bsclient.New(ctx.Context, n, bs)
					n.Start(downloadClient)

					data, err := mdinc.Download2(ctx.Context, mh, routerUrl, h, downloadClient)
					if err != nil {
						return err
					}

					if outputPath := ctx.Path("output"); outputPath != "" {
						return os.WriteFile(outputPath, data, os.ModePerm)
					}
					if _, err := os.Stdout.Write(data); err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:        "create-advertisement",
				Usage:       "<proofCID>",
				Description: "Create an advertisement containing the proof. Only for SHA256 advertisements. Format is experimental.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "multibase-encoding",
						Usage: "encode the advertisement in multibase (using a name like `base16` or a prefix like `f`)",
					},
				},
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 1 {
						return errors.New("invalid number of arguments")
					}
					cidStr := ctx.Args().Get(0)

					c, err := cid.Decode(cidStr)
					if err != nil {
						return err
					}

					ad, err := routing.CreateSHA256ProofProviderRecord(c)
					if err != nil {
						return err
					}
					if ctx.IsSet("multibase-encoding") {
						encStr := ctx.String("multibase-encoding")
						enc, err := multibase.EncoderByName(encStr)
						if err != nil {
							return err
						}
						fmt.Println(enc.Encode(ad))
					} else {
						fmt.Printf(string(ad))
					}
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

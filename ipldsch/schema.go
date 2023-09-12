package ipldsch

import (
	_ "embed"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

//go:embed .\schema.ipldsch
var embeddedSchema []byte

var Prototypes schemaSlab

type schemaSlab struct {
	Message          schema.TypedPrototype
	Entry            schema.TypedPrototype
	Send             schema.TypedPrototype
	OutboardMetadata schema.TypedPrototype
}

func init() {
	ts, err := ipld.LoadSchemaBytes(embeddedSchema)
	if err != nil {
		panic(err)
	}

	Prototypes.Message = bindnode.Prototype(
		(*Message)(nil),
		ts.TypeByName("Message"),
	)

	Prototypes.Entry = bindnode.Prototype(
		(*Entry)(nil),
		ts.TypeByName("Entry"),
	)

	Prototypes.Send = bindnode.Prototype(
		(*Send)(nil),
		ts.TypeByName("Send"),
	)

	Prototypes.OutboardMetadata = bindnode.Prototype(
		(*OutboardMetadata)(nil),
		ts.TypeByName("OutboardMetadata"),
	)
}

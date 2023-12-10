package main

import (
	pulsar "github.com/ViniciusMiana/conduit-connector-pulsar"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func main() {
	sdk.Serve(pulsar.Connector)
}

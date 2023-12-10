package pulsar

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Destination struct {
	sdk.UnimplementedDestination

	config   DestinationConfig
	client   pulsar.Client
	producer pulsar.Producer
}

type DestinationConfig struct {
	// Config includes parameters that are the same in the source and destination.
	Config
	// DestinationConfigParam must be either yes or no (defaults to yes).
	DestinationConfigParam string `validate:"inclusion=yes|no" default:"yes"`
}

// ProduceResult is the result of producing a record in a synchronous manner.
type ProduceResult struct {
	// Record is the produced record. It is always non-nil.
	Record *pulsar.ProducerMessage
	// Err is a potential produce error. If this is non-nil, the record was
	// not produced successfully.
	Err error
}

// ProduceResults is a collection of produce results.
type ProduceResults []ProduceResult

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination. Parameters can be generated from DestinationConfig with
	// paramgen.
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: d.config.URL})
	if err != nil {
		fmt.Println(err)
		return err
	}
	d.client = client
	opts := pulsar.ProducerOptions{Topic: d.config.Topic}
	prod, err := d.client.CreateProducer(opts)
	if err == nil {
		d.producer = prod
	}
	return err
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).
	var (
		wg      sync.WaitGroup
		results = make(ProduceResults, 0, len(records))
		promise = func(m pulsar.MessageID, r *pulsar.ProducerMessage, err error) {
			results = append(results, ProduceResult{Record: r, Err: err})
			wg.Done()
		}
	)

	wg.Add(len(records))
	for _, r := range records {
		pm := pulsar.ProducerMessage{
			Key:     hex.EncodeToString(r.Key.Bytes()),
			Payload: r.Bytes(),
		}
		d.producer.SendAsync(ctx, &pm, promise)
	}
	wg.Wait()

	for i, r := range results {
		if r.Err != nil {
			return i, r.Err
		}
	}
	return len(results), nil

}

func (d *Destination) Teardown(ctx context.Context) error {
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	if d.producer != nil {
		d.producer.Close()
	}
	if d.client != nil {
		d.client.Close()
	}
	return nil
}

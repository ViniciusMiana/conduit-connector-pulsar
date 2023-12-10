package pulsar

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config           SourceConfig
	lastPositionRead sdk.Position //nolint:unused // this is just an example

	client pulsar.Client
	reader pulsar.Consumer
}

type SourceConfig struct {
	// Config includes parameters that are the same in the source and destination.
	Config
}

func NewSource() sdk.Source {
	// Create Source and wrap it in the default middleware.
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	// Parameters is a map of named Parameters that describe how to configure
	// the Source. Parameters can be generated from SourceConfig with paramgen.
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.

	sdk.Logger(ctx).Info().Msg("Configuring Source...")
	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start producing records. If needed, the plugin should open connections in
	// this function. The position parameter will contain the position of the
	// last record that was successfully processed, Source should therefore
	// start producing records after this position. The context passed to Open
	// will be cancelled once the plugin receives a stop signal from Conduit.
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: s.config.URL})
	if err != nil {
		return err
	}
	s.client = client
	reader, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:               s.config.Topic,
		SubscriptionName:    uuid.NewString(),
		NackRedeliveryDelay: 2 * time.Second,
		Type:                pulsar.Shared,
		AckGroupingOptions: &pulsar.AckGroupingOptions{
			MaxSize: 100,
			MaxTime: 10,
		},
	})
	if err == nil {
		s.reader = reader
	}
	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	// Read returns a new Record and is supposed to block until there is either
	// a new record or the context gets cancelled. It can also return the error
	// ErrBackoffRetry to signal to the SDK it should call Read again with a
	// backoff retry.
	// If Read receives a cancelled context or the context is cancelled while
	// Read is running it must stop retrieving new records from the source
	// system and start returning records that have already been buffered. If
	// there are no buffered records left Read must return the context error to
	// signal a graceful stop. If Read returns ErrBackoffRetry while the context
	// is cancelled it will also signal that there are no records left and Read
	// won't be called again.
	// After Read returns an error the function won't be called again (except if
	// the error is ErrBackoffRetry, as mentioned above).
	// Read can be called concurrently with Ack.
	msg, err := s.reader.Receive(ctx)
	if err != nil {
		return sdk.Record{}, err
	}
	sdkMsg := sdk.Util.Source.NewRecordCreate(
		msg.ID().Serialize(),
		nil,
		sdk.RawData(msg.Key()),
		sdk.RawData(msg.Payload()),
	)
	return sdkMsg, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	// Ack signals to the implementation that the record with the supplied
	// position was successfully processed. This method might be called after
	// the context of Read is already cancelled, since there might be
	// outstanding acks that need to be delivered. When Teardown is called it is
	// guaranteed there won't be any more calls to Ack.
	// Ack can be called concurrently with Read.
	// s.reader.AckID(position)
	mid, err := pulsar.DeserializeMessageID(position)
	if err != nil {
		return err
	}
	return s.reader.AckID(mid)
}

func (s *Source) Teardown(ctx context.Context) error {
	// Teardown signals to the plugin that there will be no more calls to any
	// other function. After Teardown returns, the plugin should be ready for a
	// graceful shutdown.
	if s.reader != nil {
		s.reader.Close()
	}
	if s.client != nil {
		s.client.Close()
	}

	return nil
}

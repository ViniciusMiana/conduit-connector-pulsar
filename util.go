package pulsar

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

// timeout is the default timeout used in tests when interacting with Pulsar.
const timeout = 5 * time.Second

func ConfigMap(t *testing.T) map[string]string {
	lastSlash := strings.LastIndex(t.Name(), "/")
	topic := t.Name()[lastSlash+1:] + uuid.NewString()
	t.Logf("using topic: %v", topic)
	return map[string]string{
		"url":   "pulsar://localhost:6650",
		"topic": topic,
	}
}

func ParseConfigMap(t *testing.T, cfg map[string]string) Config {
	is := is.New(t)
	is.Helper()

	var out Config
	err := sdk.Util.ParseConfig(cfg, &out)
	is.NoErr(err)

	return out
}

func Consume(t *testing.T, cfg Config, limit int) []pulsar.Message {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	is := is.New(t)
	is.Helper()

	cl, err :=
		pulsar.NewClient(pulsar.ClientOptions{URL: cfg.URL})
	is.NoErr(err)
	defer cl.Close()

	var records []pulsar.Message

	reader, err := cl.CreateReader(pulsar.ReaderOptions{
		Topic:          cfg.Topic,
		StartMessageID: pulsar.EarliestMessageID(),
	})
	is.NoErr(err)
	defer reader.Close()
	count := 0
	for reader.HasNext() && count <= limit {
		count++
		msg, err := reader.Next(ctx)
		is.NoErr(err)
		records = append(records, msg)
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))
	}
	return records[:limit]
}

func Produce(t *testing.T, cfg Config, records []pulsar.ProducerMessage, timeoutOpt ...time.Duration) {
	timeout := timeout // copy default timeout
	if len(timeoutOpt) > 0 {
		timeout = timeoutOpt[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	is := is.New(t)
	is.Helper()
	cl, err :=
		pulsar.NewClient(pulsar.ClientOptions{URL: cfg.URL})
	is.NoErr(err)
	producer, err := cl.CreateProducer(pulsar.ProducerOptions{Topic: cfg.Topic,
		DisableBatching:    true,
		MaxPendingMessages: 1000,
	})
	is.NoErr(err)
	defer cl.Close()
	for i := range records {
		_, err := producer.Send(ctx, &records[i])
		is.NoErr(err)
	}
}

func GeneratePulsarRecords(from, to int) []pulsar.ProducerMessage {
	recs := make([]pulsar.ProducerMessage, 0, to-from+1)
	for i := from; i <= to; i++ {
		recs = append(recs, pulsar.ProducerMessage{
			Key:     fmt.Sprintf("test-key-%d", i),
			Payload: []byte(fmt.Sprintf("test-payload-%d", i)),
		})
	}
	return recs
}

func GenerateSDKRecords(from, to int) []sdk.Record {
	recs := GeneratePulsarRecords(from, to)
	sdkRecs := make([]sdk.Record, len(recs))
	for i, rec := range recs {
		// TODO check meta-data
		metadata := sdk.Metadata{}
		metadata.SetCreatedAt(rec.EventTime)

		sdkRecs[i] = sdk.Util.Source.NewRecordCreate(
			[]byte(uuid.NewString()),
			metadata,
			sdk.RawData(rec.Key),
			sdk.RawData(rec.Payload),
		)
	}
	fmt.Println("Generate records", sdkRecs[0].Bytes())
	return sdkRecs
}

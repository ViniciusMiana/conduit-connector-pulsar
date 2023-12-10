package pulsar

import (
	"context"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestSource_Integration_RestartFull(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cfgMap := ConfigMap(t)
	cfg := ParseConfigMap(t, cfgMap)
	underTest := initSource(t, ctx, cfgMap)
	is := is.New(t)
	defer func() {
		err := underTest.Teardown(ctx)
		is.NoErr(err)
	}()

	recs1 := GeneratePulsarRecords(1, 3)
	Produce(t, cfg, recs1)
	testSourceIntegrationRead(t, ctx, underTest, recs1, false)

	// produce more records and restart source from last position
	recs2 := GeneratePulsarRecords(4, 6)
	Produce(t, cfg, recs2)
	testSourceIntegrationRead(t, ctx, underTest, recs2, false)
}

func TestSource_Integration_RestartPartial(t *testing.T) {
	t.Skip("This test will not work due to a limitation in go-client and some design changes")
	// https://github.com/apache/pulsar-client-go/issues/403
	ctx := context.Background()

	cfgMap := ConfigMap(t)
	cfg := ParseConfigMap(t, cfgMap)
	underTest := initSource(t, ctx, cfgMap)
	is := is.New(t)
	defer func() {
		err := underTest.Teardown(ctx)
		is.NoErr(err)
	}()

	recs1 := GeneratePulsarRecords(1, 3)
	Produce(t, cfg, recs1)
	testSourceIntegrationRead(t, ctx, underTest, recs1, true)

	underTest2 := initSource(t, ctx, cfgMap)
	defer func() {
		err := underTest2.Teardown(ctx)
		is.NoErr(err)
	}()
	// only first record was acked, produce more records and expect to resume
	// from last acked record
	recs2 := GeneratePulsarRecords(4, 6)
	Produce(t, cfg, recs2)

	var wantRecs []pulsar.ProducerMessage
	wantRecs = append(wantRecs, recs1[1:]...)
	wantRecs = append(wantRecs, recs2...)
	testSourceIntegrationRead(t, ctx, underTest2, wantRecs, false)
}

// testSourceIntegrationRead reads and acks messages in range [from,to].
// If ackFirst is true, only the first message will be acknowledged.
// Returns the position of the last message read.
func testSourceIntegrationRead(
	t *testing.T,
	ctx context.Context,
	underTest sdk.Source,
	wantRecords []pulsar.ProducerMessage,
	ackFirstOnly bool,
) sdk.Position {
	is := is.New(t)

	var positions []sdk.Position
	for _, wantRecord := range wantRecords {
		rec, err := underTest.Read(ctx)
		is.NoErr(err)
		is.Equal(wantRecord.Key, string(rec.Key.Bytes()))

		positions = append(positions, rec.Position)
	}

	for i, p := range positions {
		if i > 0 && ackFirstOnly {
			break
		}
		err := underTest.Ack(ctx, p)
		is.NoErr(err)
	}

	return positions[len(positions)-1]
}

func initSource(t *testing.T, ctx context.Context, cfgMap map[string]string) sdk.Source {
	is := is.New(t)
	underTest := NewSource()

	err := underTest.Configure(ctx, cfgMap)
	is.NoErr(err)
	mid := pulsar.EarliestMessageID()
	err = underTest.Open(ctx, mid.Serialize())
	is.NoErr(err)
	return underTest
}

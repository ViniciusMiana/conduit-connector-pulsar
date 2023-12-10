package pulsar

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/matryer/is"
)

func TestClient(t *testing.T) {
	topicName := "my-topic"
	is := is.New(t)
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})
	is.NoErr(err)

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topicName,
		BatchingMaxSize: uint(100),
		DisableBatching: false,
	})
	is.NoErr(err)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: uuid.NewString(),
	})
	is.NoErr(err)
	defer consumer.Close()

	var (
		wg      sync.WaitGroup
		promise = func(m pulsar.MessageID, r *pulsar.ProducerMessage, err error) {
			if err != nil {
				fmt.Println("Failed to publish message", err)
			} else {
				fmt.Println("Published message")
			}
			wg.Done()
		}
	)
	wg.Add(1)
	producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
		Key:     string([]byte("hello")),
		Payload: []byte("hello"),
	}, promise)
	wg.Wait()

	_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte("hello"),
	})

	defer producer.Close()
	if err != nil {
		fmt.Println("Failed to publish message", err)
	} else {
		fmt.Println("Published message")
	}
	count := 0
	for i := 0; i < 2; i++ {
		msg, err := consumer.Receive(context.Background())
		is.NoErr(err)
		consumer.Nack(msg)
		count++
	}
	fmt.Println(count)
}

package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const readWriteTimeout = 10 * time.Second

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.Hash{}, // key-based partitioning keeps per-key ordering
			BatchTimeout: 10 * time.Millisecond,
			ReadTimeout:  readWriteTimeout,
			WriteTimeout: readWriteTimeout,
		},
	}
}

func (p *Producer) Publish(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error {
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: kafkaHeaders,
	})
}

type Message = kafka.Message

type ConsumerGroup struct {
	reader   *kafka.Reader
	consumed metric.Int64Counter
	consErrs metric.Int64Counter
}

func NewConsumerGroup(brokers []string, groupID string, topics []string) (*ConsumerGroup, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         brokers,
		GroupID:         groupID,
		GroupTopics:     topics,
		Dialer:          kafka.DefaultDialer,
		StartOffset:     kafka.FirstOffset,
		ReadLagInterval: -1,
	})
	meter := otel.GetMeterProvider().Meter("sixtributed")
	consumed, _ := meter.Int64Counter("kafka_consumed_total",
		metric.WithDescription("Messages consumed and committed."))
	consErrs, _ := meter.Int64Counter("kafka_consume_errors_total",
		metric.WithDescription("Consume loop errors (fetch, handler, commit)."))
	return &ConsumerGroup{reader: reader, consumed: consumed, consErrs: consErrs}, nil
}

func (c *ConsumerGroup) Close() error {
	return c.reader.Close()
}

func (c *ConsumerGroup) Consume(ctx context.Context, handler func(ctx context.Context, msg kafka.Message) error) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			c.countError(ctx)
			return err
		}
		if err := handler(ctx, msg); err != nil {
			c.countError(ctx)
			return err // caller decides retry/park; offset not committed
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.countError(ctx)
			return err
		}
		c.consumed.Add(ctx, 1)
	}
}

func (c *ConsumerGroup) countError(ctx context.Context) {
	if c.consErrs != nil {
		c.consErrs.Add(ctx, 1)
	}
}

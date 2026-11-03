package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"telco-stream/internal/domain"

	"github.com/segmentio/kafka-go"
)

// Producer mesaj gönderme arayüzüdür.
type Producer interface {
	PublishCDR(ctx context.Context, event domain.CDREvent) error
	Close() error
}

type kafkaProducer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // bildigimiz hash, topicleri parcalara ayirip performans kazanmak icin
		BatchSize:    100,           // toplu gonderilecek veri siniri
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne, // Leader broker diske yazdığında onay al ?
	}

	return &kafkaProducer{
		writer: writer,
	}
}

// PublishCDR CDR olayını JSON formatında Kafka'ya yazar.
func (p *kafkaProducer) PublishCDR(ctx context.Context, event domain.CDREvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal cdr event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.SubscriberID), // Sıralama garantisi için kritik!
		Value: payload,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	return nil
}

// Close producer bağlantısını güvenle kapatır.
func (p *kafkaProducer) Close() error {
	return p.writer.Close()
}

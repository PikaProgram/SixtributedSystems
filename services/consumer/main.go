package main

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/astolfo/sixtributedsystems/internal/platform"
	"github.com/rabbitmq/amqp091-go"
)

type envelope struct {
	EventType string         `json:"event_type"`
	Event     map[string]any `json:"event"`
}

func main() {
	service := platform.Env("CONSUMER_NAME", "consumer")
	logger := platform.Logger(service)
	go func() {
		_ = http.ListenAndServe(
			":"+platform.Env("HEALTH_PORT", "8083"),
			http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				platform.JSON(
					w,
					http.StatusOK,
					map[string]string{"status": "ok", "service": service},
				)
			}),
		)
	}()
	url := platform.Env("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	queue := platform.Env("CONSUMER_QUEUE", service+".queue")
	for {
		if err := consume(url, queue, logger); err != nil {
			logger.Error("consumer_cycle", "error", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func consume(url, queue string, logger interface {
	Info(string, ...any)
	Error(string, ...any)
}) error {
	conn, err := amqp091.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(
		"hazard.events",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(queue, "hazard.created", "hazard.events", false, nil); err != nil {
		return err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	messages, err := ch.Consume(queue, "", false, false, false, false, nil)
	logger.Info("started", "queue", queue)
	for message := range messages {
		var event envelope
		if err := json.Unmarshal(message.Body, &event); err != nil {
			logger.Error("invalid_event", "error", err)
			_ = message.Nack(false, false)
			continue
		}
		logger.Info("event_processed", "event_type", event.EventType,
			"hazard_id", event.Event["hazard_id"], "correlation_id", event.Event["correlation_id"])
		_ = message.Ack(false)
	}
	return os.ErrClosed
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ordersExchange = "orders_exchange"
	statusExchange = "status_exchange"
	ordersQueue    = "orders_queue"
	statusQueue    = "status_queue"
	ordersKey      = "order"
	statusKey      = "status"
)

type OrderMessage struct {
	MissionID   string `json:"mission_id"`
	Description string `json:"description"`
}

type StatusMessage struct {
	MissionID string `json:"mission_id"`
	Status    string `json:"status"`
	Token     string `json:"token"`
	SoldierID string `json:"soldier_id"`
}

type Rabbit struct {
	url  string
	log  *slog.Logger
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewRabbit(url string, log *slog.Logger) *Rabbit {
	return &Rabbit{url: url, log: log}
}

func (r *Rabbit) Connect(ctx context.Context) error {
	var last error
	for i := 0; i < 30; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn, err := amqp.Dial(r.url)
		if err != nil {
			last = err
			r.log.Warn("rabbitmq dial failed, retrying", "err", err, "attempt", i+1)
			time.Sleep(2 * time.Second)
			continue
		}
		ch, err := conn.Channel()
		if err != nil {
			_ = conn.Close()
			last = err
			time.Sleep(2 * time.Second)
			continue
		}
		if err := declareTopology(ch); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			last = err
			time.Sleep(2 * time.Second)
			continue
		}
		r.conn = conn
		r.ch = ch
		r.log.Info("connected to rabbitmq")
		return nil
	}
	return fmt.Errorf("rabbitmq connect failed: %w", last)
}

func declareTopology(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ordersExchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(statusExchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(ordersQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(statusQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(ordersQueue, ordersKey, ordersExchange, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(statusQueue, statusKey, statusExchange, false, nil)
}

func (r *Rabbit) PublishOrder(ctx context.Context, msg OrderMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	var last error
	for i := 0; i < 3; i++ {
		err = r.ch.PublishWithContext(ctx, ordersExchange, ordersKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		})
		if err == nil {
			return nil
		}
		last = err
		time.Sleep(200 * time.Millisecond)
	}
	return last
}

func (r *Rabbit) ConsumeStatus(ctx context.Context, handler func(context.Context, StatusMessage) error) error {
	if err := r.ch.Qos(10, 0, false); err != nil {
		return err
	}
	deliveries, err := r.ch.Consume(statusQueue, "commander-status", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("status channel closed")
			}
			var msg StatusMessage
			if err := json.Unmarshal(d.Body, &msg); err != nil {
				r.log.Error("bad status payload", "err", err)
				_ = d.Ack(false)
				continue
			}
			// The handler returns an error only when the update is worth
			// retrying (for example the database is down). Messages it
			// rejects on purpose come back as nil so we stop redelivering.
			if err := handler(ctx, msg); err != nil {
				r.log.Error("status update failed, requeueing",
					"err", err, "mission_id", msg.MissionID, "status", msg.Status)
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

func (r *Rabbit) Close() {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
}

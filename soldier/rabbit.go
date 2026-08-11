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

func (r *Rabbit) ConsumeOrders(prefetch int) (<-chan amqp.Delivery, error) {
	if err := r.ch.Qos(prefetch, 0, false); err != nil {
		return nil, err
	}
	return r.ch.Consume(ordersQueue, "", false, false, false, false, nil)
}

func (r *Rabbit) PublishStatus(ctx context.Context, msg StatusMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return r.ch.PublishWithContext(ctx, statusExchange, statusKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (r *Rabbit) Close() {
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
}

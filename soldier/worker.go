package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type WorkerPool struct {
	size      int
	minDelay  time.Duration
	maxDelay  time.Duration
	soldierID string
	tokens    *TokenManager
	rabbit    *Rabbit
	log       *slog.Logger
	jobs      chan amqp.Delivery
	wg        sync.WaitGroup
}

func NewWorkerPool(size int, minDelay, maxDelay time.Duration, soldierID string, tokens *TokenManager, rabbit *Rabbit, log *slog.Logger) *WorkerPool {
	if size < 1 {
		size = 1
	}
	return &WorkerPool{
		size:      size,
		minDelay:  minDelay,
		maxDelay:  maxDelay,
		soldierID: soldierID,
		tokens:    tokens,
		rabbit:    rabbit,
		log:       log,
		jobs:      make(chan amqp.Delivery, size),
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		go func(workerID int) {
			defer p.wg.Done()
			for d := range p.jobs {
				p.process(ctx, workerID, d)
			}
		}(i)
	}
}

func (p *WorkerPool) Submit(d amqp.Delivery) {
	p.jobs <- d
}

// Stop closes the job channel and waits for workers to finish queued work.
func (p *WorkerPool) Stop() {
	close(p.jobs)
	p.wg.Wait()
}

func (p *WorkerPool) process(ctx context.Context, workerID int, d amqp.Delivery) {
	var order OrderMessage
	if err := json.Unmarshal(d.Body, &order); err != nil {
		p.log.Error("bad order payload", "err", err)
		_ = d.Ack(false)
		return
	}

	start := time.Now()
	p.log.Info("mission received",
		"mission_id", order.MissionID,
		"soldier_id", p.soldierID,
		"worker", workerID,
	)

	if err := p.publishStatus(ctx, order.MissionID, "IN_PROGRESS"); err != nil {
		p.log.Error("publish IN_PROGRESS failed", "err", err, "mission_id", order.MissionID)
		_ = d.Nack(false, true)
		return
	}

	delay := p.minDelay
	if p.maxDelay > p.minDelay {
		delay = p.minDelay + time.Duration(rand.Int63n(int64(p.maxDelay-p.minDelay)+1))
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		_ = d.Nack(false, true)
		return
	case <-timer.C:
	}

	final := "COMPLETED"
	if rand.Float64() >= 0.9 {
		final = "FAILED"
	}

	if err := p.publishStatus(ctx, order.MissionID, final); err != nil {
		p.log.Error("publish final status failed", "err", err, "mission_id", order.MissionID, "status", final)
		_ = d.Nack(false, true)
		return
	}

	if err := d.Ack(false); err != nil {
		p.log.Error("ack failed", "err", err, "mission_id", order.MissionID)
		return
	}

	p.log.Info("mission finished",
		"mission_id", order.MissionID,
		"soldier_id", p.soldierID,
		"status", final,
		"duration", time.Since(start).String(),
	)
}

func (p *WorkerPool) publishStatus(ctx context.Context, missionID, status string) error {
	token, tokenID, err := p.tokens.GetValidToken(ctx)
	if err != nil {
		return err
	}
	if err := p.rabbit.PublishStatus(ctx, StatusMessage{
		MissionID: missionID,
		Status:    status,
		Token:     token,
		SoldierID: p.soldierID,
	}); err != nil {
		return err
	}
	p.log.Info("status published",
		"mission_id", missionID,
		"soldier_id", p.soldierID,
		"status", status,
		"token_id", tokenID,
	)
	return nil
}

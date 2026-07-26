/*
 * Copyright 2026 Scott Walter, MMFP Solutions LLC
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.  See LICENSE for more details.
 */

package noderpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/ForgeNX/forgenx-engine/pkg/logging"
	"github.com/go-zeromq/zmq4"
)

// ZMQSubscriber listens for hashblock notifications from a blockchain node via ZMQ.
type ZMQSubscriber struct {
	endpoint string
	logger   *logging.Logger
	onBlock  func(blockHash string)
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewZMQSubscriber creates a new ZMQ subscriber for hashblock events.
// The onBlock callback is invoked with the block hash whenever a new block is detected.
func NewZMQSubscriber(endpoint string, onBlock func(blockHash string)) *ZMQSubscriber {
	return &ZMQSubscriber{
		endpoint: endpoint,
		logger:   logging.New(logging.ModuleZMQ),
		onBlock:  onBlock,
	}
}

// Start connects to the ZMQ endpoint and begins listening for hashblock
// notifications. The initial connection must succeed; after that, transient
// errors (e.g. the node restarting) trigger automatic reconnection with
// backoff rather than permanently killing the subscriber.
func (z *ZMQSubscriber) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	z.cancel = cancel

	// Require the first connection to succeed so misconfiguration is caught
	// at startup rather than silently degrading to poll-only.
	sub, err := z.dial(ctx)
	if err != nil {
		cancel()
		return err
	}

	z.wg.Add(1)
	go func() {
		defer z.wg.Done()
		z.runLoop(ctx, sub)
	}()

	return nil
}

// dial opens a fresh SUB socket subscribed to hashblock.
func (z *ZMQSubscriber) dial(ctx context.Context) (zmq4.Socket, error) {
	sub := zmq4.NewSub(ctx)
	if err := sub.Dial(z.endpoint); err != nil {
		return nil, fmt.Errorf("connecting to ZMQ endpoint %s: %w", z.endpoint, err)
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, "hashblock"); err != nil {
		sub.Close()
		return nil, fmt.Errorf("subscribing to hashblock topic: %w", err)
	}
	z.logger.Info("ZMQ subscriber connected to %s", z.endpoint)
	return sub, nil
}

// runLoop listens on the current socket and, when listen returns due to a
// non-shutdown error, reconnects with capped exponential backoff. Exits only
// when the context is cancelled (Stop was called).
func (z *ZMQSubscriber) runLoop(ctx context.Context, sub zmq4.Socket) {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		z.listen(ctx, sub)
		sub.Close()

		// If we're shutting down, stop; otherwise reconnect.
		select {
		case <-ctx.Done():
			return
		default:
		}

		z.logger.Warn("ZMQ connection lost — reconnecting in %s", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		newSub, err := z.dial(ctx)
		if err != nil {
			z.logger.Error("ZMQ reconnect failed: %v", err)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		sub = newSub
		backoff = time.Second // reset after a successful reconnect
	}
}

func (z *ZMQSubscriber) listen(ctx context.Context, sub zmq4.Socket) {
	for {
		msg, err := sub.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				z.logger.Info("ZMQ subscriber shutting down")
				return
			default:
				z.logger.Error("ZMQ recv error: %v", err)
				return
			}
		}

		// ZMQ hashblock message has 3 frames: topic, body (32-byte hash), sequence
		if len(msg.Frames) < 2 {
			continue
		}

		topic := string(msg.Frames[0])
		if topic != "hashblock" {
			continue
		}

		blockHash := hex.EncodeToString(msg.Frames[1])
		z.logger.Debug("ZMQ hashblock received: %s", blockHash)
		z.onBlock(blockHash)
	}
}

// Stop disconnects the ZMQ subscriber.
func (z *ZMQSubscriber) Stop() {
	if z.cancel != nil {
		z.cancel()
	}
	z.wg.Wait()
	z.logger.Info("ZMQ subscriber stopped")
}

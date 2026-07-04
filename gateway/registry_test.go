// MIT License
//
// Copyright (c) 2022-2026 GoAkt Team
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package gateway_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/gateway"
	"github.com/tochemey/goakt/v4/internal/pause"
	"github.com/tochemey/goakt/v4/log"
)

func newTestSystem(t *testing.T, opts ...actor.Option) actor.ActorSystem {
	t.Helper()
	ctx := context.Background()
	allOpts := append([]actor.Option{actor.WithLogger(log.DiscardLogger)}, opts...)
	system, err := actor.NewActorSystem(t.Name(), allOpts...)
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))
	t.Cleanup(func() {
		_ = system.Stop(context.Background())
	})
	pause.For(100 * time.Millisecond)
	return system
}

func TestRegistrySendToConnectionLocalHit(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	var mu sync.Mutex
	var received [][]byte
	send := func(payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, payload)
		return nil
	}

	ctx := context.Background()
	require.NoError(t, registry.Register(ctx, "conn-1", send))
	require.True(t, registry.Has("conn-1"))

	require.NoError(t, registry.SendToConnection(ctx, "conn-1", []byte("hello")))

	mu.Lock()
	require.Len(t, received, 1)
	require.Equal(t, []byte("hello"), received[0])
	mu.Unlock()

	require.NoError(t, registry.Unregister(ctx, "conn-1"))
	require.False(t, registry.Has("conn-1"))
}

func TestRegistrySendToConnectionMiss(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	err := registry.SendToConnection(context.Background(), "does-not-exist", []byte("hello"))
	require.ErrorIs(t, err, gateway.ErrConnectionNotFound)
}

func TestRegistryRegisterTwiceFails(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	send := func([]byte) error { return nil }
	ctx := context.Background()
	require.NoError(t, registry.Register(ctx, "dup", send))

	err := registry.Register(ctx, "dup", send)
	require.ErrorIs(t, err, gateway.ErrConnectionExists)

	require.NoError(t, registry.Unregister(ctx, "dup"))
}

func TestRegistryBroadcastLocalMembersOnly(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	var mu sync.Mutex
	received := map[string]int{}
	makeSend := func(id string) func([]byte) error {
		return func([]byte) error {
			mu.Lock()
			defer mu.Unlock()
			received[id]++
			return nil
		}
	}

	require.NoError(t, registry.Register(ctx, "a", makeSend("a"), "room-1"))
	require.NoError(t, registry.Register(ctx, "b", makeSend("b"), "room-1"))
	require.NoError(t, registry.Register(ctx, "c", makeSend("c")))

	require.NoError(t, registry.Broadcast(ctx, "room-1", []byte("hi")))

	pause.For(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, received["a"])
	require.Equal(t, 1, received["b"])
	require.Equal(t, 0, received["c"])
}

func TestRegistryJoinLeave(t *testing.T) {
	system := newTestSystem(t, actor.WithPubSub())
	registry := gateway.NewRegistry(system, log.DiscardLogger)
	ctx := context.Background()

	var count int
	var mu sync.Mutex
	send := func([]byte) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	}

	require.NoError(t, registry.Register(ctx, "leaver", send))
	require.NoError(t, registry.Join(ctx, "leaver", "topic-x"))

	require.NoError(t, registry.Broadcast(ctx, "topic-x", []byte("one")))
	pause.For(200 * time.Millisecond)

	require.NoError(t, registry.Leave(ctx, "leaver", "topic-x"))
	require.NoError(t, registry.Broadcast(ctx, "topic-x", []byte("two")))
	pause.For(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, count)
}

func TestRegistryJoinUnknownConnection(t *testing.T) {
	system := newTestSystem(t)
	registry := gateway.NewRegistry(system, log.DiscardLogger)

	err := registry.Join(context.Background(), "ghost", "topic")
	require.ErrorIs(t, err, gateway.ErrConnectionNotFound)
}

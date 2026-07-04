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

package testkit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/internal/pause"
	"github.com/tochemey/goakt/v4/kv"
	"github.com/tochemey/goakt/v4/log"
)

// TestKVCluster exercises the cluster-scoped KV registry and distributed lock
// (actor.ActorSystem.KV) across multiple nodes.
func TestKVCluster(t *testing.T) {
	ctx := context.Background()

	t.Run("PutIfAbsent race has exactly one winner", func(t *testing.T) {
		multi := NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&pinger{}}, nil)
		multi.Start()
		t.Cleanup(multi.Stop)

		const nodeCount = 3
		nodes := make([]*TestNode, nodeCount)
		for i := range nodeCount {
			nodes[i] = multi.StartNode(ctx, "putifabsent-node")
		}

		var wg sync.WaitGroup
		results := make([]error, nodeCount)
		for i, node := range nodes {
			wg.Add(1)
			go func(i int, node *TestNode) {
				defer wg.Done()
				store, err := node.ActorSystem().KV()
				require.NoError(t, err)
				results[i] = store.PutIfAbsent(ctx, "registry:widget", []byte(node.NodeName()))
			}(i, node)
		}
		wg.Wait()

		successes := 0
		for _, err := range results {
			if err == nil {
				successes++
			} else {
				require.ErrorIs(t, err, kv.ErrKeyExists)
			}
		}
		require.Equal(t, 1, successes, "exactly one node must win the PutIfAbsent race")
	})

	t.Run("TryLock race has exactly one winner", func(t *testing.T) {
		multi := NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&pinger{}}, nil)
		multi.Start()
		t.Cleanup(multi.Stop)

		const nodeCount = 3
		nodes := make([]*TestNode, nodeCount)
		for i := range nodeCount {
			nodes[i] = multi.StartNode(ctx, "trylock-node")
		}

		var wg sync.WaitGroup
		locks := make([]kv.Lock, nodeCount)
		errs := make([]error, nodeCount)
		for i, node := range nodes {
			wg.Add(1)
			go func(i int, node *TestNode) {
				defer wg.Done()
				store, err := node.ActorSystem().KV()
				require.NoError(t, err)
				locks[i], errs[i] = store.TryLock(ctx, "cert:example.com", 10*time.Second)
			}(i, node)
		}
		wg.Wait()

		successes := 0
		for i, err := range errs {
			if err == nil {
				successes++
				require.NotNil(t, locks[i])
			} else {
				require.ErrorIs(t, err, kv.ErrLockNotAcquired)
			}
		}
		require.Equal(t, 1, successes, "exactly one node must win the lock race")
	})

	t.Run("TTL expiry", func(t *testing.T) {
		multi := NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&pinger{}}, nil)
		multi.Start()
		t.Cleanup(multi.Stop)

		node := multi.StartNode(ctx, "ttl-node")
		store, err := node.ActorSystem().KV()
		require.NoError(t, err)

		require.NoError(t, store.Put(ctx, "dedup:msg-1", []byte("seen"), kv.WithTTL(300*time.Millisecond)))

		value, err := store.Get(ctx, "dedup:msg-1")
		require.NoError(t, err)
		require.Equal(t, []byte("seen"), value)

		pause.For(700 * time.Millisecond)

		_, err = store.Get(ctx, "dedup:msg-1")
		require.ErrorIs(t, err, kv.ErrKeyNotFound)
	})

	t.Run("Unlock then relock from another node", func(t *testing.T) {
		multi := NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&pinger{}}, nil)
		multi.Start()
		t.Cleanup(multi.Stop)

		node1 := multi.StartNode(ctx, "unlock-node-1")
		node2 := multi.StartNode(ctx, "unlock-node-2")

		store1, err := node1.ActorSystem().KV()
		require.NoError(t, err)
		store2, err := node2.ActorSystem().KV()
		require.NoError(t, err)

		lock1, err := store1.TryLock(ctx, "job:nightly", 10*time.Second)
		require.NoError(t, err)
		require.NotNil(t, lock1)

		// node2 cannot acquire the lock while node1 holds it
		_, err = store2.TryLock(ctx, "job:nightly", 10*time.Second)
		require.ErrorIs(t, err, kv.ErrLockNotAcquired)

		require.NoError(t, lock1.Unlock(ctx))

		// once released, node2 can now acquire it
		lock2, err := store2.TryLock(ctx, "job:nightly", 10*time.Second)
		require.NoError(t, err)
		require.NotNil(t, lock2)
		require.NoError(t, lock2.Unlock(ctx))
	})
}

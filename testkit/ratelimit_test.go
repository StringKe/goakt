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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/ratelimit"
)

// TestRateLimitCluster exercises ratelimit.Limiter across multiple nodes sharing the
// same cluster kv.Store, asserting that the aggregate admitted request count for a key
// stays within the configured bound regardless of which node served each request.
func TestRateLimitCluster(t *testing.T) {
	ctx := context.Background()

	t.Run("aggregate admissions across nodes respect the configured limit", func(t *testing.T) {
		multi := NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&pinger{}}, nil)
		multi.Start()
		t.Cleanup(multi.Stop)

		const nodeCount = 3
		nodes := make([]*TestNode, nodeCount)
		for i := range nodeCount {
			nodes[i] = multi.StartNode(ctx, "ratelimit-node")
		}

		// A window long enough that the test cannot cross a window boundary keeps
		// this assertion about concurrency, not about the fixed-window rollover.
		const limit = 30
		const window = 30 * time.Second

		limiters := make([]*ratelimit.Limiter, nodeCount)
		for i, node := range nodes {
			store, err := node.ActorSystem().KV()
			require.NoError(t, err)
			limiter, err := ratelimit.New(store, limit, window)
			require.NoError(t, err)
			limiters[i] = limiter
		}

		const attemptsPerNode = 20 // nodeCount*attemptsPerNode = 60 > limit, so the limiter must reject some.
		var admitted atomic.Int64
		var wg sync.WaitGroup
		for _, limiter := range limiters {
			for range attemptsPerNode {
				wg.Add(1)
				go func(limiter *ratelimit.Limiter) {
					defer wg.Done()
					ok, err := limiter.Allow(ctx, "shared-endpoint")
					require.NoError(t, err)
					if ok {
						admitted.Add(1)
					}
				}(limiter)
			}
		}
		wg.Wait()

		// The fixed-window counter is a single cluster-shared entry guarded by a
		// distributed lock: a documented tolerance above the strict limit absorbs
		// lock-retry edge cases without hiding a limiter that is not bounding load at
		// all (which would admit close to all nodeCount*attemptsPerNode attempts).
		const tolerance = 5
		got := admitted.Load()
		require.LessOrEqualf(t, got, int64(limit+tolerance),
			"admitted %d requests, want at most %d (limit=%d + tolerance=%d)", got, limit+tolerance, limit, tolerance)
		require.Greater(t, got, int64(0), "the limiter must admit some requests")
		require.Less(t, got, int64(nodeCount*attemptsPerNode), "the limiter must reject some requests once the limit is exceeded")
	})

	t.Run("different keys have independent budgets across nodes", func(t *testing.T) {
		multi := NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&pinger{}}, nil)
		multi.Start()
		t.Cleanup(multi.Stop)

		node1 := multi.StartNode(ctx, "ratelimit-node-1")
		node2 := multi.StartNode(ctx, "ratelimit-node-2")

		store1, err := node1.ActorSystem().KV()
		require.NoError(t, err)
		store2, err := node2.ActorSystem().KV()
		require.NoError(t, err)

		limiter1, err := ratelimit.New(store1, 1, time.Minute)
		require.NoError(t, err)
		limiter2, err := ratelimit.New(store2, 1, time.Minute)
		require.NoError(t, err)

		ok, err := limiter1.Allow(ctx, "tenant-a")
		require.NoError(t, err)
		require.True(t, ok)

		// tenant-a is now exhausted cluster-wide, observed from the other node.
		ok, err = limiter2.Allow(ctx, "tenant-a")
		require.NoError(t, err)
		require.False(t, ok)

		// tenant-b has its own budget, unaffected by tenant-a's usage.
		ok, err = limiter2.Allow(ctx, "tenant-b")
		require.NoError(t, err)
		require.True(t, ok)
	})
}

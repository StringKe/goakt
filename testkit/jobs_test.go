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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/internal/internalpb"
	"github.com/tochemey/goakt/v4/internal/pause"
	"github.com/tochemey/goakt/v4/jobs"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
	"github.com/tochemey/goakt/v4/test/data/testpb"
)

// jobsWorker replies to *testpb.TestSum with the sum of its two operands. It
// plays the role of "compute one downline member's reward" in the fan-out
// cluster test below.
type jobsWorker struct{}

func (jobsWorker) PreStart(*actor.Context) error { return nil }
func (jobsWorker) PostStop(*actor.Context) error { return nil }
func (jobsWorker) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *testpb.TestSum:
		ctx.Response(&testpb.TestSumResult{Result: msg.GetA() + msg.GetB()})
	default:
		ctx.Unhandled()
	}
}

// jobsAggregator sums the TestSumResult payloads carried by a
// *internalpb.FanInEnvelope, playing the role of "total downline reward".
type jobsAggregator struct {
	mu    sync.Mutex
	total int64
	calls int
}

func (a *jobsAggregator) PreStart(*actor.Context) error { return nil }
func (a *jobsAggregator) PostStop(*actor.Context) error { return nil }
func (a *jobsAggregator) Receive(ctx *actor.ReceiveContext) {
	envelope, ok := ctx.Message().(*internalpb.FanInEnvelope)
	if !ok {
		ctx.Unhandled()
		return
	}

	var total int64
	for _, r := range envelope.GetResults() {
		if r.GetFailed() {
			ctx.Response(errors.New(r.GetError()))
			return
		}
		decoded, err := remote.NewProtoSerializer().Deserialize(r.GetPayload())
		if err != nil {
			ctx.Response(err)
			return
		}
		sumResult, ok := decoded.(*testpb.TestSumResult)
		if !ok {
			ctx.Response(errors.New("unexpected child result type"))
			return
		}
		total += sumResult.GetResult()
	}

	a.mu.Lock()
	a.total = total
	a.calls++
	a.mu.Unlock()
	ctx.Response(&testpb.TestSumResult{Result: total})
}

func (a *jobsAggregator) snapshot() (int64, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total, a.calls
}

// TestJobsLeaseTakeoverAfterNodeCrash proves the crash-safety invariant a
// durable Store must uphold: a job leased by a node that then disappears
// (kill -9, in production) is not lost -- once its lease expires, any other
// engine sharing the Store redelivers it, and delivery completes normally
// through the still-alive target actor elsewhere in the cluster.
func TestJobsLeaseTakeoverAfterNodeCrash(t *testing.T) {
	ctx := context.Background()

	multi := NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&pinger{}, &jobsWorker{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	multi.StartNode(ctx, "jobs-crash-node-1")
	node2 := multi.StartNode(ctx, "jobs-crash-node-2")
	pause.For(2 * time.Second)

	node2.Spawn(ctx, "worker", &jobsWorker{})

	// A durable Store implementation is expected to be shared across every
	// process/node in the cluster; NewMemoryStore is process-local, so this
	// test stands one in for that role by sharing a single Go value across
	// the two nodes' Engines, exactly like a real SQL/Redis-backed Store
	// would be reachable from every node.
	store := jobs.NewMemoryStore()

	job, err := jobs.NewJob("rewards", "worker", &testpb.TestSum{A: 20, B: 22})
	require.NoError(t, err)
	require.NoError(t, store.Enqueue(ctx, job))

	// Simulate node-1 leasing the job and then crashing before it can
	// Ack/Nack: lease it directly with a short TTL, then tear the node down
	// for real.
	leased, err := store.Lease(ctx, "rewards", "jobs-crash-node-1", 200*time.Millisecond, 1)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.Equal(t, job.ID, leased[0].ID)

	multi.StopNode(ctx, "jobs-crash-node-1")

	// node-2's engine shares the Store: once the lease expires it must
	// reclaim and successfully redeliver the job to the worker actor that
	// only ever existed on node-2.
	engine, err := jobs.NewEngine(node2.ActorSystem(), jobs.EngineConfig{
		Store:        store,
		PollInterval: 20 * time.Millisecond,
		LeaseTTL:     time.Second,
		Logger:       log.DiscardLogger,
	})
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(context.Background()) })

	require.Eventually(t, func() bool {
		got, getErr := store.Get(ctx, job.ID)
		return getErr == nil && got.State == jobs.StateSucceeded
	}, 10*time.Second, 100*time.Millisecond, "the job must be redelivered and completed after its original leasing node crashes")
}

// TestJobsFanOutAcrossNodes exercises the first-class fan-out/fan-in path
// over a real cluster: a parent job's children are distributed across three
// nodes (each racing Store.Lease for them), and once every child completes,
// the parent's reduce step -- also delivered cluster-wide -- aggregates their
// results. This mirrors the business case the feature was built for:
// computing an agent's total downline rewards.
func TestJobsFanOutAcrossNodes(t *testing.T) {
	ctx := context.Background()

	multi := NewMultiNodes(t, log.DiscardLogger, []actor.Actor{&pinger{}, &jobsWorker{}}, nil)
	multi.Start()
	t.Cleanup(multi.Stop)

	node1 := multi.StartNode(ctx, "jobs-fanout-node-1")
	node2 := multi.StartNode(ctx, "jobs-fanout-node-2")
	node3 := multi.StartNode(ctx, "jobs-fanout-node-3")
	pause.For(2 * time.Second)

	// Actor names are cluster-wide identities, so each worker needs a distinct
	// name; "worker-N" computes one downline member's reward, one per node,
	// so children can be delivered without leaving whichever node happens to
	// lease them.
	workerNames := []string{"worker-1", "worker-2", "worker-3"}
	node1.Spawn(ctx, workerNames[0], &jobsWorker{})
	node2.Spawn(ctx, workerNames[1], &jobsWorker{})
	node3.Spawn(ctx, workerNames[2], &jobsWorker{})

	aggregator := &jobsAggregator{}
	node1.Spawn(ctx, "aggregator", aggregator)

	store := jobs.NewMemoryStore()

	engines := make([]*jobs.Engine, 3)
	for i, node := range []*TestNode{node1, node2, node3} {
		engine, err := jobs.NewEngine(node.ActorSystem(), jobs.EngineConfig{
			Store:        store,
			PollInterval: 20 * time.Millisecond,
			LeaseTTL:     2 * time.Second,
			Logger:       log.DiscardLogger,
		})
		require.NoError(t, err)
		require.NoError(t, engine.Start(ctx))
		engines[i] = engine
	}
	t.Cleanup(func() {
		for _, engine := range engines {
			_ = engine.Stop(context.Background())
		}
	})

	parent, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{})
	require.NoError(t, err)

	rewards := [][2]int64{{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}} // sums: 3,7,11,15,19 -> total 55
	children := make([]*jobs.Job, len(rewards))
	for i, r := range rewards {
		children[i], err = jobs.NewJob("rewards", workerNames[i%len(workerNames)], &testpb.TestSum{A: r[0], B: r[1]})
		require.NoError(t, err)
	}

	require.NoError(t, store.EnqueueFanOut(ctx, parent, children))

	require.Eventually(t, func() bool {
		got, getErr := store.Get(ctx, parent.ID)
		return getErr == nil && got.State == jobs.StateSucceeded
	}, 15*time.Second, 100*time.Millisecond, "the fan-out parent must complete once every child, wherever it landed, has succeeded")

	total, calls := aggregator.snapshot()
	require.Equal(t, int64(55), total)
	require.Equal(t, 1, calls)
}

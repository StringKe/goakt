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

package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/internal/internalpb"
	"github.com/tochemey/goakt/v4/jobs"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
	"github.com/tochemey/goakt/v4/test/data/testpb"
)

// sumActor replies to *testpb.TestSum with the sum of its two operands. It
// stands in for the "compute one downline member's reward" business unit of
// work in the fan-out tests.
type sumActor struct{}

func (sumActor) PreStart(*actor.Context) error { return nil }
func (sumActor) PostStop(*actor.Context) error { return nil }
func (sumActor) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *testpb.TestSum:
		ctx.Response(&testpb.TestSumResult{Result: msg.GetA() + msg.GetB()})
	default:
		ctx.Unhandled()
	}
}

// aggregatorActor sums the TestSumResult payloads carried by a
// *internalpb.FanInEnvelope, standing in for the "total downline reward"
// reduce step.
type aggregatorActor struct {
	mu    sync.Mutex
	total int64
	calls int
}

func (a *aggregatorActor) PreStart(*actor.Context) error { return nil }
func (a *aggregatorActor) PostStop(*actor.Context) error { return nil }
func (a *aggregatorActor) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *internalpb.FanInEnvelope:
		var total int64
		for _, r := range msg.GetResults() {
			if r.GetFailed() {
				ctx.Response(fmt.Errorf("child %d failed: %s", r.GetChildIndex(), r.GetError()))
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
	default:
		ctx.Unhandled()
	}
}

func (a *aggregatorActor) snapshot() (int64, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total, a.calls
}

// flakyActor fails the first failuresBeforeSuccess deliveries, then succeeds.
type flakyActor struct {
	remaining atomic.Int64
}

func newFlakyActor(failuresBeforeSuccess int64) *flakyActor {
	a := &flakyActor{}
	a.remaining.Store(failuresBeforeSuccess)
	return a
}

func (a *flakyActor) PreStart(*actor.Context) error { return nil }
func (a *flakyActor) PostStop(*actor.Context) error  { return nil }
func (a *flakyActor) Receive(ctx *actor.ReceiveContext) {
	switch ctx.Message().(type) {
	case *testpb.TestSum:
		if a.remaining.Load() > 0 {
			a.remaining.Add(-1)
			ctx.Response(errors.New("transient failure"))
			return
		}
		ctx.Response(&testpb.TestSumResult{Result: 1})
	default:
		ctx.Unhandled()
	}
}

// alwaysFailActor always fails, to exercise dead-lettering.
type alwaysFailActor struct{}

func (alwaysFailActor) PreStart(*actor.Context) error { return nil }
func (alwaysFailActor) PostStop(*actor.Context) error { return nil }
func (alwaysFailActor) Receive(ctx *actor.ReceiveContext) {
	switch ctx.Message().(type) {
	case *testpb.TestSum:
		ctx.Response(errors.New("permanent failure"))
	default:
		ctx.Unhandled()
	}
}

func newTestActorSystem(t *testing.T) actor.ActorSystem {
	t.Helper()
	ctx := context.Background()
	system, err := actor.NewActorSystem(fmt.Sprintf("jobs-test-%d", time.Now().UnixNano()), actor.WithLogger(log.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))
	t.Cleanup(func() {
		_ = system.Stop(context.Background())
	})
	return system
}

func TestEngine_DeliversAndAcks(t *testing.T) {
	ctx := context.Background()
	system := newTestActorSystem(t)
	_, err := system.Spawn(ctx, "sum", &sumActor{})
	require.NoError(t, err)

	store := jobs.NewMemoryStore()
	engine, err := jobs.NewEngine(system, jobs.EngineConfig{
		Store:        store,
		PollInterval: 20 * time.Millisecond,
		LeaseTTL:     time.Second,
		Logger:       log.DiscardLogger,
	})
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(context.Background()) })

	job, err := jobs.NewJob("rewards", "sum", &testpb.TestSum{A: 2, B: 3})
	require.NoError(t, err)
	require.NoError(t, engine.Enqueue(ctx, job))

	require.Eventually(t, func() bool {
		got, err := store.Get(ctx, job.ID)
		return err == nil && got.State == jobs.StateSucceeded
	}, 2*time.Second, 20*time.Millisecond)
}

func TestEngine_RetriesThenSucceeds(t *testing.T) {
	ctx := context.Background()
	system := newTestActorSystem(t)
	_, err := system.Spawn(ctx, "flaky", newFlakyActor(2))
	require.NoError(t, err)

	store := jobs.NewMemoryStore()
	engine, err := jobs.NewEngine(system, jobs.EngineConfig{
		Store:        store,
		PollInterval: 10 * time.Millisecond,
		LeaseTTL:     time.Second,
		Logger:       log.DiscardLogger,
	})
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(context.Background()) })

	job, err := jobs.NewJob("rewards", "flaky", &testpb.TestSum{A: 1, B: 1},
		jobs.WithRetryPolicy(jobs.FixedBackoff(15*time.Millisecond, 5)))
	require.NoError(t, err)
	require.NoError(t, engine.Enqueue(ctx, job))

	require.Eventually(t, func() bool {
		got, err := store.Get(ctx, job.ID)
		return err == nil && got.State == jobs.StateSucceeded
	}, 2*time.Second, 10*time.Millisecond)

	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, 2, got.Attempts)
}

func TestEngine_DeadLettersAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	system := newTestActorSystem(t)
	_, err := system.Spawn(ctx, "always-fail", alwaysFailActor{})
	require.NoError(t, err)

	store := jobs.NewMemoryStore()
	engine, err := jobs.NewEngine(system, jobs.EngineConfig{
		Store:        store,
		PollInterval: 10 * time.Millisecond,
		LeaseTTL:     time.Second,
		Logger:       log.DiscardLogger,
	})
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(context.Background()) })

	job, err := jobs.NewJob("rewards", "always-fail", &testpb.TestSum{A: 1, B: 1},
		jobs.WithRetryPolicy(jobs.FixedBackoff(10*time.Millisecond, 3)))
	require.NoError(t, err)
	require.NoError(t, engine.Enqueue(ctx, job))

	require.Eventually(t, func() bool {
		got, err := store.Get(ctx, job.ID)
		return err == nil && got.State == jobs.StateDeadLetter
	}, 2*time.Second, 10*time.Millisecond)

	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, 3, got.Attempts)
}

func TestEngine_FanOutAggregatesDownlineRewards(t *testing.T) {
	ctx := context.Background()
	system := newTestActorSystem(t)
	_, err := system.Spawn(ctx, "sum", &sumActor{})
	require.NoError(t, err)
	aggregator := &aggregatorActor{}
	_, err = system.Spawn(ctx, "aggregator", aggregator)
	require.NoError(t, err)

	store := jobs.NewMemoryStore()
	engine, err := jobs.NewEngine(system, jobs.EngineConfig{
		Store:        store,
		PollInterval: 10 * time.Millisecond,
		LeaseTTL:     time.Second,
		Logger:       log.DiscardLogger,
	})
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(context.Background()) })

	parent, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{})
	require.NoError(t, err)

	// three "downline members", each contributing A+B to the total reward.
	rewards := [][2]int64{{1, 2}, {3, 4}, {5, 6}} // sums: 3, 7, 11 -> total 21
	children := make([]*jobs.Job, len(rewards))
	for i, r := range rewards {
		children[i], err = jobs.NewJob("rewards", "sum", &testpb.TestSum{A: r[0], B: r[1]})
		require.NoError(t, err)
	}

	require.NoError(t, engine.FanOut(ctx, parent, children))

	require.Eventually(t, func() bool {
		got, err := store.Get(ctx, parent.ID)
		return err == nil && got.State == jobs.StateSucceeded
	}, 2*time.Second, 10*time.Millisecond)

	total, calls := aggregator.snapshot()
	require.Equal(t, int64(21), total)
	require.Equal(t, 1, calls)
}

func TestNewEngine_RequiresActorSystemAndStore(t *testing.T) {
	_, err := jobs.NewEngine(nil, jobs.EngineConfig{Store: jobs.NewMemoryStore()})
	require.ErrorIs(t, err, jobs.ErrActorSystemRequired)
}

func TestEngine_StartTwiceErrors(t *testing.T) {
	ctx := context.Background()
	system := newTestActorSystem(t)
	engine, err := jobs.NewEngine(system, jobs.EngineConfig{Store: jobs.NewMemoryStore()})
	require.NoError(t, err)
	require.NoError(t, engine.Start(ctx))
	t.Cleanup(func() { _ = engine.Stop(context.Background()) })
	require.ErrorIs(t, engine.Start(ctx), jobs.ErrEngineAlreadyStarted)
}

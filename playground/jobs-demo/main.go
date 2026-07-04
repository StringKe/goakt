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

// jobs-demo shows the jobs package's two headline capabilities working
// together: a cron schedule (the actor system's existing ScheduleWithCron)
// periodically kicks off a fan-out/fan-in computation run through the jobs
// Engine -- the business case the feature was built for, computing an
// agent's total reward across a fixed set of "downline" members.
//
// Each tick fans a parent job out into one child job per downline agent,
// computed independently by a reward-worker actor, and aggregates the results
// back into the parent's target once every child completes. Since the set of
// downline agent IDs never changes, the aggregated total is deterministic
// across every tick, which is what makes this sample self-validating: it runs
// for a bounded window, then asserts at least two rounds completed with
// exactly the expected total and exits non-zero if not.
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/internal/pause"
	"github.com/tochemey/goakt/v4/jobs"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
)

// downlineAgentIDs is the fixed set of "downline members" whose rewards get
// aggregated on every tick. rewardWorker's computation is a deterministic
// function of the agent id, so the expected total below never changes.
var downlineAgentIDs = []string{"agent-1", "agent-22", "agent-333"}

// expectedTotal is downlineAgentIDs' rewards summed by rewardFor: 700+800+900.
const expectedTotal = int64(2400)

// rewardFor deterministically computes a downline member's reward so the demo
// can assert on an exact total.
func rewardFor(agentID string) int64 {
	return int64(len(agentID)) * 100
}

// rewardWorker computes one downline member's reward.
type rewardWorker struct{}

func (rewardWorker) PreStart(*actor.Context) error { return nil }
func (rewardWorker) PostStop(*actor.Context) error { return nil }
func (rewardWorker) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case *wrapperspb.StringValue:
		ctx.Response(&wrapperspb.Int64Value{Value: rewardFor(msg.GetValue())})
	default:
		ctx.Unhandled()
	}
}

// rewardAggregator sums every downline member's reward once a fan-out round
// completes, standing in for "an agent's total downline reward".
type rewardAggregator struct {
	mu    sync.Mutex
	total int64
	runs  int
}

func (a *rewardAggregator) PreStart(*actor.Context) error { return nil }
func (a *rewardAggregator) PostStop(*actor.Context) error { return nil }
func (a *rewardAggregator) Receive(ctx *actor.ReceiveContext) {
	envelope, ok := ctx.Message().(*jobs.FanInEnvelope)
	if !ok {
		ctx.Unhandled()
		return
	}

	var total int64
	for _, result := range envelope.GetResults() {
		if result.GetFailed() {
			ctx.Response(fmt.Errorf("child %d failed: %s", result.GetChildIndex(), result.GetError()))
			return
		}
		decoded, err := remote.NewProtoSerializer().Deserialize(result.GetPayload())
		if err != nil {
			ctx.Response(err)
			return
		}
		value, ok := decoded.(*wrapperspb.Int64Value)
		if !ok {
			ctx.Response(fmt.Errorf("unexpected child result type %T", decoded))
			return
		}
		total += value.GetValue()
	}

	a.mu.Lock()
	a.total = total
	a.runs++
	a.mu.Unlock()

	fmt.Printf("[aggregator] fan-out round complete: total reward = %d\n", total)
	ctx.Response(&wrapperspb.Int64Value{Value: total})
}

func (a *rewardAggregator) snapshot() (total int64, runs int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total, a.runs
}

// cronTrigger fans a new reward-aggregation round out every time it receives
// a cron tick.
type cronTrigger struct {
	engine *jobs.Engine
}

func (t *cronTrigger) PreStart(*actor.Context) error { return nil }
func (t *cronTrigger) PostStop(*actor.Context) error { return nil }
func (t *cronTrigger) Receive(ctx *actor.ReceiveContext) {
	switch ctx.Message().(type) {
	case *emptypb.Empty:
		if err := t.fanOut(ctx.Context()); err != nil {
			fmt.Printf("[cron] failed to start a fan-out round: %v\n", err)
		}
	default:
		ctx.Unhandled()
	}
}

func (t *cronTrigger) fanOut(ctx context.Context) error {
	parent, err := jobs.NewJob("rewards", "reward-aggregator", &emptypb.Empty{})
	if err != nil {
		return err
	}

	children := make([]*jobs.Job, len(downlineAgentIDs))
	for i, agentID := range downlineAgentIDs {
		children[i], err = jobs.NewJob("rewards", "reward-worker", &wrapperspb.StringValue{Value: agentID})
		if err != nil {
			return err
		}
	}

	fmt.Printf("[cron] tick: starting a fan-out round over %d downline agents\n", len(children))
	return t.engine.FanOut(ctx, parent, children)
}

func main() {
	ctx := context.Background()

	actorSystem, err := actor.NewActorSystem("jobs-demo", actor.WithLogger(log.DiscardLogger))
	must(err)
	must(actorSystem.Start(ctx))
	defer func() { _ = actorSystem.Stop(ctx) }()

	_, err = actorSystem.Spawn(ctx, "reward-worker", rewardWorker{})
	must(err)
	aggregator := &rewardAggregator{}
	_, err = actorSystem.Spawn(ctx, "reward-aggregator", aggregator)
	must(err)

	store := jobs.NewMemoryStore()
	engine, err := jobs.NewEngine(actorSystem, jobs.EngineConfig{
		Store:        store,
		PollInterval: 100 * time.Millisecond,
		LeaseTTL:     10 * time.Second,
		Logger:       log.DiscardLogger,
	})
	must(err)
	must(engine.Start(ctx))
	defer func() { _ = engine.Stop(ctx) }()

	trigger, err := actorSystem.Spawn(ctx, "cron-trigger", &cronTrigger{engine: engine})
	must(err)
	must(actorSystem.ScheduleWithCron(ctx, &emptypb.Empty{}, trigger, "*/2 * * * * *"))

	fmt.Println("running for 7s: expect at least two fan-out rounds, each totalling", expectedTotal)
	pause.For(7 * time.Second)

	total, runs := aggregator.snapshot()
	depths, err := engine.Inspector().QueueDepths(ctx)
	must(err)
	fmt.Printf("completed %d round(s), last total = %d, queue depths = %v\n", runs, total, depths)

	if runs < 2 {
		fmt.Println("FAIL: expected at least 2 completed fan-out rounds")
		os.Exit(1)
	}
	if total != expectedTotal {
		fmt.Printf("FAIL: expected total reward %d, got %d\n", expectedTotal, total)
		os.Exit(1)
	}

	fmt.Println("PASS")
}

func must(err error) {
	if err != nil {
		fmt.Println("FAIL:", err)
		os.Exit(1)
	}
}

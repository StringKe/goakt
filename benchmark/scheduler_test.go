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

package benchmark

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/reugn/go-quartz/quartz"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/test/data/testpb"
)

// benchmarkScheduleOnce measures the throughput of registering ScheduleOnce
// jobs. The delay is deliberately long (1h) so no job actually fires during
// the run: what is being measured is the registration path (building the job,
// and — when queue is non-nil — serializing a ScheduledMessage envelope
// through the remoting pipeline), not delivery.
//
// Registration goes through the scheduler's own mutex, so parallel callers
// would just serialize on that lock; the benchmark runs sequentially to
// report a clean per-call cost instead of measuring lock contention.
func benchmarkScheduleOnce(b *testing.B, queue quartz.JobQueue, locker sync.Locker) {
	ctx := context.Background()

	opts := []actor.Option{
		actor.WithLogger(log.DiscardLogger),
		actor.WithActorInitMaxRetries(1),
	}
	if queue != nil {
		opts = append(opts, actor.WithSchedulerJobQueue(queue, locker))
	}

	actorSystem, err := actor.NewActorSystem("bench-schedule", opts...)
	if err != nil {
		b.Fatalf("failed to create actor system: %v", err)
	}
	if err := actorSystem.Start(ctx); err != nil {
		b.Fatalf("failed to start actor system: %v", err)
	}
	b.Cleanup(func() { _ = actorSystem.Stop(ctx) })

	receiver, err := actorSystem.Spawn(ctx, "receiver", new(Actor))
	if err != nil {
		b.Fatalf("failed to spawn receiver: %v", err)
	}

	msg := new(testpb.TestSend)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := actorSystem.ScheduleOnce(ctx, msg, receiver, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	schedulesPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(schedulesPerSec, "schedules/sec")
}

// BenchmarkScheduleOnce measures registration throughput against the default,
// unconfigured in-memory job queue.
func BenchmarkScheduleOnce(b *testing.B) {
	benchmarkScheduleOnce(b, nil, nil)
}

// BenchmarkScheduleOncePersistentQueue measures registration throughput with
// WithSchedulerJobQueue configured, isolating the added cost of building and
// serializing a ScheduledMessage envelope through the remoting pipeline. It
// uses go-quartz's own exported in-memory quartz.NewJobQueue as a stand-in
// external queue: GoAkt ships no concrete persistent implementation, but the
// envelope-building/serialization path exercised here is identical regardless
// of which JobQueue backs it.
func BenchmarkScheduleOncePersistentQueue(b *testing.B) {
	benchmarkScheduleOnce(b, quartz.NewJobQueue(), &sync.Mutex{})
}

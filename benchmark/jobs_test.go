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
	"fmt"
	"testing"
	"time"

	"github.com/tochemey/goakt/v4/jobs"
	"github.com/tochemey/goakt/v4/test/data/testpb"
)

// BenchmarkJobsEnqueue measures NewMemoryStore().Enqueue throughput: building
// a job (which serializes its payload through the remoting pipeline) plus
// recording it. Run sequentially, since memoryStore serializes every call on
// a single mutex; what is measured is the per-call cost of that critical
// section, not lock contention.
func BenchmarkJobsEnqueue(b *testing.B) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	message := &testpb.TestSum{A: 1, B: 2}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job, err := jobs.NewJob("bench", "receiver", message, jobs.WithID(fmt.Sprintf("job-%d", i)))
		if err != nil {
			b.Fatal(err)
		}
		if err := store.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	perSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(perSec, "enqueues/sec")
}

// BenchmarkJobsLease measures NewMemoryStore().Lease throughput against a
// store pre-populated with b.N due jobs, leasing one job per call: the
// realistic access pattern for an Engine's poll loop. Lease scans every known
// job (documented as an in-memory-reference-implementation limitation), so
// this also reports how that scan cost scales with store size.
func BenchmarkJobsLease(b *testing.B) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	message := &testpb.TestSum{A: 1, B: 2}

	for i := 0; i < b.N; i++ {
		job, err := jobs.NewJob("bench", "receiver", message, jobs.WithID(fmt.Sprintf("job-%d", i)))
		if err != nil {
			b.Fatal(err)
		}
		if err := store.Enqueue(ctx, job); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Lease(ctx, "bench", "worker", time.Minute, 1); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	perSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(perSec, "leases/sec")
}

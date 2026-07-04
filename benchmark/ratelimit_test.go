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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/ratelimit"
)

// BenchmarkRateLimitAllowDistinctKeys measures Allow throughput when every call
// targets a fresh key, so each call takes the PutIfAbsent fast path without ever
// contending on the counter lock.
func BenchmarkRateLimitAllowDistinctKeys(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping cluster benchmark in short mode")
	}

	ctx := context.Background()
	store := newBenchKVCluster(b)
	limiter, err := ratelimit.New(store, 1, time.Minute)
	require.NoError(b, err)

	b.ResetTimer()
	for i := range b.N {
		if _, err := limiter.Allow(ctx, "bench:ratelimit:distinct:"+strconv.Itoa(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRateLimitAllowSharedKey measures Allow throughput when every call targets
// the same key, exercising the lock-guarded read-modify-write slow path once the
// window's counter already exists.
func BenchmarkRateLimitAllowSharedKey(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping cluster benchmark in short mode")
	}

	ctx := context.Background()
	store := newBenchKVCluster(b)
	limiter, err := ratelimit.New(store, b.N+1, time.Hour)
	require.NoError(b, err)

	b.ResetTimer()
	for range b.N {
		if _, err := limiter.Allow(ctx, "bench:ratelimit:shared"); err != nil {
			b.Fatal(err)
		}
	}
}

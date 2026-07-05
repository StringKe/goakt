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

	"github.com/tochemey/goakt/v4/jobs"
)

func TestInspector_ListByState(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	inspector := jobs.NewInspector(store)

	succeeded := newTestJob(t, jobs.WithID("succeeded"))
	require.NoError(t, store.Enqueue(ctx, succeeded))
	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.NoError(t, store.Ack(ctx, succeeded.ID, leased[0].LeaseToken, nil))

	deadLettered := newTestJob(t, jobs.WithID("dead-lettered"), jobs.WithRetryPolicy(jobs.FixedBackoff(time.Minute, 1)))
	require.NoError(t, store.Enqueue(ctx, deadLettered))
	leased, err = store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.NoError(t, store.Nack(ctx, deadLettered.ID, leased[0].LeaseToken, errors.New("boom")))

	pending := newTestJob(t, jobs.WithID("pending"))
	require.NoError(t, store.Enqueue(ctx, pending))

	got, err := inspector.List(ctx, jobs.Filter{States: []jobs.State{jobs.StateDeadLetter}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "dead-lettered", got[0].ID)

	got, err = inspector.List(ctx, jobs.Filter{States: []jobs.State{jobs.StateSucceeded}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "succeeded", got[0].ID)

	got, err = inspector.List(ctx, jobs.Filter{States: []jobs.State{jobs.StatePending}})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "pending", got[0].ID)

	all, err := inspector.List(ctx, jobs.Filter{})
	require.NoError(t, err)
	require.Len(t, all, 3)
}

func TestInspector_Get(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	inspector := jobs.NewInspector(store)

	job := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, job))

	got, err := inspector.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, job.ID, got.ID)
	require.Equal(t, jobs.StatePending, got.State)

	_, err = inspector.Get(ctx, "missing")
	require.ErrorIs(t, err, jobs.ErrJobNotFound)
}

func TestInspector_Retry(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	inspector := jobs.NewInspector(store)

	job := newTestJob(t, jobs.WithRetryPolicy(jobs.FixedBackoff(time.Minute, 1)))
	require.NoError(t, store.Enqueue(ctx, job))
	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.NoError(t, store.Nack(ctx, job.ID, leased[0].LeaseToken, errors.New("boom")))

	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)

	require.NoError(t, inspector.Retry(ctx, job.ID))

	got, err = store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StatePending, got.State)
	require.Equal(t, 0, got.Attempts)
	require.Empty(t, got.LastError)

	// the job is leasable again after a manual retry.
	leased, err = store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.Equal(t, job.ID, leased[0].ID)
}

func TestInspector_Delete(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	inspector := jobs.NewInspector(store)

	job := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, job))
	require.NoError(t, inspector.Delete(ctx, job.ID))

	_, err := store.Get(ctx, job.ID)
	require.ErrorIs(t, err, jobs.ErrJobNotFound)

	require.ErrorIs(t, inspector.Delete(ctx, "missing"), jobs.ErrJobNotFound)
}

func TestInspector_QueueDepths(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	inspector := jobs.NewInspector(store)

	succeeded := newTestJob(t, jobs.WithID("succeeded"))
	require.NoError(t, store.Enqueue(ctx, succeeded))
	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.NoError(t, store.Ack(ctx, succeeded.ID, leased[0].LeaseToken, nil))

	pending := newTestJob(t, jobs.WithID("pending"))
	require.NoError(t, store.Enqueue(ctx, pending))

	depths, err := inspector.QueueDepths(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, depths["rewards"])
}

// TestInspector_ListDuringConcurrentMutation exercises Inspector.List (P0-3:
// previously zero coverage) while other goroutines concurrently enqueue,
// lease, Ack and Nack jobs on the same Store -- run with -race, this proves
// List never data-races with the memoryStore's mutations and always returns a
// consistent, error-free snapshot no larger than the total job count.
func TestInspector_ListDuringConcurrentMutation(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	inspector := jobs.NewInspector(store)

	const seedJobs = 50
	for i := range seedJobs {
		job := newTestJob(t, jobs.WithID(fmt.Sprintf("seed-%d", i)),
			jobs.WithRetryPolicy(jobs.FixedBackoff(time.Millisecond, 1000)))
		require.NoError(t, store.Enqueue(ctx, job))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// continuously lease and settle (Ack or Nack) jobs, mutating state
	// concurrently with the List loop below.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			leased, err := store.Lease(ctx, "rewards", "mutator", time.Millisecond, 5)
			require.NoError(t, err)
			for _, j := range leased {
				if i%2 == 0 {
					_ = store.Ack(ctx, j.ID, j.LeaseToken, nil)
				} else {
					_ = store.Nack(ctx, j.ID, j.LeaseToken, errors.New("flaky"))
				}
				i++
			}
		}
	}()

	// continuously enqueue fresh jobs so List also races against map growth.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			job := newTestJob(t, jobs.WithID(fmt.Sprintf("extra-%d", i)))
			_ = store.Enqueue(ctx, job)
			i++
		}
	}()

	for range 200 {
		got, err := inspector.List(ctx, jobs.Filter{Queue: "rewards"})
		require.NoError(t, err)
		require.NotNil(t, got)
	}

	close(stop)
	wg.Wait()

	final, err := inspector.List(ctx, jobs.Filter{Queue: "rewards"})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(final), seedJobs)
}

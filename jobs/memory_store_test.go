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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/jobs"
	"github.com/tochemey/goakt/v4/test/data/testpb"
)

func newTestJob(t *testing.T, opts ...jobs.JobOption) *jobs.Job {
	t.Helper()
	job, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{A: 1, B: 2}, opts...)
	require.NoError(t, err)
	return job
}

func TestMemoryStore_EnqueueLease(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()

	job := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, job))
	require.ErrorIs(t, store.Enqueue(ctx, job), jobs.ErrJobAlreadyExists)

	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.Equal(t, jobs.StateLeased, leased[0].State)
	require.Equal(t, "worker-1", leased[0].LeaseOwner)

	// leased job is no longer returned by a concurrent lease
	again, err := store.Lease(ctx, "rewards", "worker-2", time.Minute, 10)
	require.NoError(t, err)
	require.Empty(t, again)
}

func TestMemoryStore_LeaseReclaimsExpiredLease(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, job))

	_, err := store.Lease(ctx, "rewards", "worker-1", time.Millisecond, 10)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		leased, err := store.Lease(ctx, "rewards", "worker-2", time.Minute, 10)
		return err == nil && len(leased) == 1 && leased[0].LeaseOwner == "worker-2"
	}, time.Second, 5*time.Millisecond, "an expired lease must be reclaimed by another worker")
}

func TestMemoryStore_AckIsTerminalAndRejectsRepeat(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, job))
	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	token := leased[0].LeaseToken

	require.NoError(t, store.Ack(ctx, job.ID, token, nil))
	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateSucceeded, got.State)

	// a second Ack does not resurrect or re-mutate an already-terminal job
	require.ErrorIs(t, store.Ack(ctx, job.ID, token, nil), jobs.ErrStaleLease)
	got, err = store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateSucceeded, got.State)
}

func TestMemoryStore_NackRetriesThenDeadLetters(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t, jobs.WithRetryPolicy(jobs.FixedBackoff(10*time.Millisecond, 2)))
	require.NoError(t, store.Enqueue(ctx, job))

	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.NoError(t, store.Nack(ctx, job.ID, leased[0].LeaseToken, errors.New("transient")))

	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StatePending, got.State)
	require.Equal(t, 1, got.Attempts)
	require.Equal(t, "transient", got.LastError)
	require.True(t, got.AvailableAt.After(time.Now().Add(-time.Millisecond)))

	var secondToken int64
	require.Eventually(t, func() bool {
		leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
		if err != nil || len(leased) != 1 {
			return false
		}
		secondToken = leased[0].LeaseToken
		return true
	}, time.Second, 5*time.Millisecond)

	require.NoError(t, store.Nack(ctx, job.ID, secondToken, errors.New("still failing")))
	got, err = store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)
	require.Equal(t, 2, got.Attempts)
}

func TestMemoryStore_DeadLetterIsTerminalAndRejectsRepeat(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, job))

	// never leased: token 0 matches the job's zero-value LeaseToken.
	require.NoError(t, store.DeadLetter(ctx, job.ID, 0, errors.New("boom")))
	require.ErrorIs(t, store.DeadLetter(ctx, job.ID, 0, errors.New("boom again")), jobs.ErrStaleLease)

	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)
	require.Equal(t, "boom", got.LastError)
}

func TestMemoryStore_AckAfterDeadLetterIsNoop(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t, jobs.WithRetryPolicy(jobs.FixedBackoff(time.Minute, 1)))
	require.NoError(t, store.Enqueue(ctx, job))

	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	token := leased[0].LeaseToken

	// a single attempt with MaxAttempts=1 dead-letters immediately.
	require.NoError(t, store.Nack(ctx, job.ID, token, errors.New("permanent")))
	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)

	// a late Ack from the same worker/token must not resurrect the job to
	// StateSucceeded (P0-1): the dead-letter state is final.
	require.ErrorIs(t, store.Ack(ctx, job.ID, token, nil), jobs.ErrStaleLease)
	got, err = store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)
}

func TestMemoryStore_NackAfterDeadLetterIsNoop(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t, jobs.WithRetryPolicy(jobs.FixedBackoff(time.Minute, 1)))
	require.NoError(t, store.Enqueue(ctx, job))

	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	token := leased[0].LeaseToken

	require.NoError(t, store.Nack(ctx, job.ID, token, errors.New("permanent")))
	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)
	require.Equal(t, 1, got.Attempts)

	// a late, duplicate Nack must not bump Attempts or otherwise mutate an
	// already dead-lettered job.
	require.ErrorIs(t, store.Nack(ctx, job.ID, token, errors.New("late retry")), jobs.ErrStaleLease)
	got, err = store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)
	require.Equal(t, 1, got.Attempts)
}

func TestMemoryStore_UnknownJobErrors(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()

	_, err := store.Get(ctx, "missing")
	require.ErrorIs(t, err, jobs.ErrJobNotFound)
	require.ErrorIs(t, store.Ack(ctx, "missing", 0, nil), jobs.ErrJobNotFound)
	require.ErrorIs(t, store.Nack(ctx, "missing", 0, errors.New("x")), jobs.ErrJobNotFound)
	require.ErrorIs(t, store.DeadLetter(ctx, "missing", 0, errors.New("x")), jobs.ErrJobNotFound)
	require.ErrorIs(t, store.Delete(ctx, "missing"), jobs.ErrJobNotFound)
	require.ErrorIs(t, store.Requeue(ctx, "missing"), jobs.ErrJobNotFound)
}

func TestMemoryStore_Requeue(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t, jobs.WithRetryPolicy(jobs.FixedBackoff(time.Minute, 1)))
	require.NoError(t, store.Enqueue(ctx, job))
	require.NoError(t, store.DeadLetter(ctx, job.ID, 0, errors.New("boom")))

	require.NoError(t, store.Requeue(ctx, job.ID))
	got, err := store.Get(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StatePending, got.State)
	require.Equal(t, 0, got.Attempts)
	require.Empty(t, got.LastError)
}

func TestMemoryStore_ListFilterAndPagination(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()

	for i := range 5 {
		job := newTestJob(t, jobs.WithID(time.Now().Add(time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano)))
		require.NoError(t, store.Enqueue(ctx, job))
	}

	all, err := store.List(ctx, jobs.Filter{})
	require.NoError(t, err)
	require.Len(t, all, 5)

	page, err := store.List(ctx, jobs.Filter{Limit: 2, Offset: 1})
	require.NoError(t, err)
	require.Len(t, page, 2)
	require.Equal(t, all[1].ID, page[0].ID)
	require.Equal(t, all[2].ID, page[1].ID)

	byState, err := store.List(ctx, jobs.Filter{States: []jobs.State{jobs.StateSucceeded}})
	require.NoError(t, err)
	require.Empty(t, byState)
}

func TestMemoryStore_QueueDepths(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()

	pending := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, pending))

	succeeded := newTestJob(t, jobs.WithID("succeeded"))
	require.NoError(t, store.Enqueue(ctx, succeeded))
	require.NoError(t, store.Ack(ctx, succeeded.ID, 0, nil))

	depths, err := store.QueueDepths(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, depths["rewards"])
}

func TestMemoryStore_Delete(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, job))
	require.NoError(t, store.Delete(ctx, job.ID))
	_, err := store.Get(ctx, job.ID)
	require.ErrorIs(t, err, jobs.ErrJobNotFound)
}

func TestMemoryStore_FanOutAggregatesOnAllSuccess(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()

	parent, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{})
	require.NoError(t, err)

	children := make([]*jobs.Job, 3)
	for i := range children {
		children[i], err = jobs.NewJob("rewards", "worker", &testpb.TestSum{A: int64(i)})
		require.NoError(t, err)
	}

	require.NoError(t, store.EnqueueFanOut(ctx, parent, children))

	got, err := store.Get(ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateWaiting, got.State)

	// a single lease pass picks up every child but never the waiting parent
	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, len(children))
	for _, j := range leased {
		require.NotEqual(t, parent.ID, j.ID)
	}

	for i, job := range leased {
		require.NoError(t, store.Ack(ctx, job.ID, job.LeaseToken, []byte{byte(i)}))
	}

	got, err = store.Get(ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StatePending, got.State)
	require.Len(t, got.ChildResults, 3)
	for i, r := range got.ChildResults {
		require.Equal(t, i, r.ChildIndex)
		require.False(t, r.Failed)
	}

	// the parent itself is now leasable for its reduce step
	leased, err = store.Lease(ctx, "rewards", "reducer", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.Equal(t, parent.ID, leased[0].ID)
}

func TestMemoryStore_FanOutDeadLettersParentWhenAChildFails(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()

	parent, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{})
	require.NoError(t, err)

	children := make([]*jobs.Job, 3)
	for i := range children {
		children[i], err = jobs.NewJob("rewards", "worker", &testpb.TestSum{A: int64(i)},
			jobs.WithRetryPolicy(jobs.FixedBackoff(time.Minute, 1)))
		require.NoError(t, err)
	}
	require.NoError(t, store.EnqueueFanOut(ctx, parent, children))

	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 3)

	// one child dead-letters (max attempts is 1): the parent must fail fast,
	// without waiting for its siblings.
	require.NoError(t, store.Nack(ctx, leased[0].ID, leased[0].LeaseToken, errors.New("permanent failure")))

	got, err := store.Get(ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)

	// the remaining siblings can still complete without panicking or
	// resurrecting the parent.
	require.NoError(t, store.Ack(ctx, leased[1].ID, leased[1].LeaseToken, nil))
	require.NoError(t, store.Ack(ctx, leased[2].ID, leased[2].LeaseToken, nil))

	got, err = store.Get(ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateDeadLetter, got.State)
}

func TestMemoryStore_FanOutSingleChild(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()

	parent, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{})
	require.NoError(t, err)
	child, err := jobs.NewJob("rewards", "worker", &testpb.TestSum{A: 7, B: 8})
	require.NoError(t, err)

	require.NoError(t, store.EnqueueFanOut(ctx, parent, []*jobs.Job{child}))

	got, err := store.Get(ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StateWaiting, got.State)
	require.Equal(t, 1, got.ChildCount)

	leased, err := store.Lease(ctx, "rewards", "worker-1", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.Equal(t, child.ID, leased[0].ID)

	require.NoError(t, store.Ack(ctx, leased[0].ID, leased[0].LeaseToken, []byte{42}))

	got, err = store.Get(ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, jobs.StatePending, got.State)
	require.Len(t, got.ChildResults, 1)
	require.Equal(t, 0, got.ChildResults[0].ChildIndex)
	require.Equal(t, []byte{42}, got.ChildResults[0].Payload)

	// a single child is still a fan-out: the parent must go through the
	// reduce-step lease like any other, not be short-circuited.
	leased, err = store.Lease(ctx, "rewards", "reducer", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.Equal(t, parent.ID, leased[0].ID)
}

// TestMemoryStore_LeaseBoundaryExactExpiry pins down the chosen semantics at
// the exact LeaseExpiresAt boundary: a lease is treated as expired (and thus
// reclaimable) as soon as it is not strictly in the future, i.e. the boundary
// itself counts as expired rather than requiring a full extra tick past it.
// Reclaiming also mints a fresh fencing token (see ErrStaleLease).
func TestMemoryStore_LeaseBoundaryExactExpiry(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	job := newTestJob(t)
	require.NoError(t, store.Enqueue(ctx, job))

	// leaseTTL=0 sets LeaseExpiresAt to "now" at grant time; by the time the
	// next Lease call observes it, LeaseExpiresAt is at or before the new
	// "now" -- exercising the inclusive boundary rather than a lease that
	// merely expired some time ago.
	first, err := store.Lease(ctx, "rewards", "worker-1", 0, 10)
	require.NoError(t, err)
	require.Len(t, first, 1)

	second, err := store.Lease(ctx, "rewards", "worker-2", time.Minute, 10)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, job.ID, second[0].ID)
	require.Equal(t, "worker-2", second[0].LeaseOwner)
	require.NotEqual(t, first[0].LeaseToken, second[0].LeaseToken, "reclaiming must mint a fresh fencing token")
}

func TestMemoryStore_EnqueueFanOutRejectsEmptyChildren(t *testing.T) {
	ctx := context.Background()
	store := jobs.NewMemoryStore()
	parent, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{})
	require.NoError(t, err)

	require.ErrorIs(t, store.EnqueueFanOut(ctx, parent, nil), jobs.ErrEmptyChildren)
}

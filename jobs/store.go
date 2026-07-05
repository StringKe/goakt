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

package jobs

import (
	"context"
	"time"
)

// Filter narrows List to a subset of jobs. A zero-value Filter matches every
// job. Limit <= 0 means unlimited.
type Filter struct {
	Queue    string
	States   []State
	ParentID string
	Limit    int
	Offset   int
}

// Store is the storage-agnostic persistence contract an Engine drives. The
// built-in default is NewMemoryStore; durable adapters (SQL, ...) live outside
// this package and must uphold the invariants below.
//
// Invariants implementations MUST uphold:
//
//  1. Enqueue durably records job in the state the caller set on it (normally
//     StatePending). A duplicate id returns ErrJobAlreadyExists.
//
//  2. EnqueueFanOut durably records parent (forced into StateWaiting
//     regardless of the state the caller set) and every one of children
//     (each with ParentID set to parent's id and ChildIndex set to its
//     position), atomically with respect to Lease: a partially-written
//     fan-out must never become visible.
//
//  3. Lease is atomic across concurrent callers, including across
//     process/node boundaries for shared/durable backends: a job handed to
//     one caller must not be handed to another until its lease expires or it
//     is Ack/Nack/DeadLetter'd. Lease also reclaims any job whose previous
//     LeaseExpiresAt has passed -- this is what makes a crashed worker's jobs
//     redeliver automatically. Every successful grant or reclaim assigns a new
//     LeaseToken (monotonically increasing, unique per Store instance), which
//     the caller must present back to Ack/Nack/DeadLetter: this is the fencing
//     token that stops a worker whose lease already expired and was reclaimed
//     from finalizing the job out from under its new owner. A StateWaiting
//     parent is never returned by Lease; it only becomes leasable once every
//     child has completed and it has been moved back to StatePending by the
//     child-completion logic described under Ack/Nack/DeadLetter.
//
//  4. Ack is a terminal success transition, gated by the fencing token: it
//     returns ErrStaleLease, without mutating the job, when token does not
//     match the job's current LeaseToken or the job is already in a terminal
//     state (StateSucceeded or StateDeadLetter) -- this is what stops a late
//     Ack from resurrecting a dead-lettered job or double-recording a fan-out
//     child's result. When the job has a non-empty ParentID, a successful Ack
//     additionally records result against the parent (see point 6).
//
//  5. Nack is gated by the same fencing check as Ack (ErrStaleLease on token
//     mismatch or an already-terminal job). Otherwise it increments Attempts
//     and either reschedules the job at now+RetryPolicy.NextDelay(Attempts)
//     (back to StatePending) or, once Attempts reaches
//     RetryPolicy.MaxAttempts, moves it to StateDeadLetter -- in which case it
//     cascades into the parent exactly like DeadLetter (point 6).
//
//  6. DeadLetter is gated by the same fencing check as Ack, then forces the
//     dead-letter transition regardless of Attempts. When the job has a
//     non-empty ParentID, the Store decrements the parent's outstanding child
//     counter and records the child's result. Once every child has reached a
//     terminal state, the parent leaves StateWaiting: if every child
//     succeeded, the parent moves to StatePending with ChildResults populated
//     (ordered by ChildIndex), ready for its reduce-step delivery; if any
//     child was dead-lettered, the parent moves straight to StateDeadLetter
//     without waiting for its remaining siblings -- a fan-out is only as
//     strong as its weakest child.
//
//  7. Delete removes a job (and, if the job is a fan-out parent, its
//     bookkeeping) regardless of state.
//
//  8. Requeue resets a job (typically dead-lettered) back to StatePending
//     with Attempts reset to zero and AvailableAt set to now, regardless of
//     its current LeaseToken; it is the primitive behind Inspector.Retry and
//     is distinct from the automatic retry Nack performs.
type Store interface {
	// Enqueue durably records job, ready to be leased once its AvailableAt has
	// passed.
	Enqueue(ctx context.Context, job *Job) error

	// EnqueueFanOut durably records parent and children as a linked fan-out:
	// parent will not be leasable until every child in children has reached a
	// terminal state (see invariant 6).
	EnqueueFanOut(ctx context.Context, parent *Job, children []*Job) error

	// Lease atomically claims up to limit due jobs from queue (all queues when
	// queue is empty) on behalf of owner, marking them StateLeased with
	// LeaseExpiresAt set to now+leaseTTL and a fresh LeaseToken. limit <= 0
	// means unlimited.
	Lease(ctx context.Context, queue, owner string, leaseTTL time.Duration, limit int) ([]*Job, error)

	// Ack marks id as successfully completed, provided token matches the lease
	// token Lease last handed out for it (otherwise ErrStaleLease; see
	// invariant 4). result is the handler's response payload (already
	// serialized), used only when id is a fan-out child; it is ignored
	// otherwise and may be nil.
	Ack(ctx context.Context, id string, token int64, result []byte) error

	// Nack records a failed delivery attempt for id, described by cause, and
	// either reschedules it for retry or dead-letters it, provided token
	// matches the lease token Lease last handed out for it (otherwise
	// ErrStaleLease; see invariant 5).
	Nack(ctx context.Context, id string, token int64, cause error) error

	// DeadLetter forces id into StateDeadLetter, described by cause, provided
	// token matches the lease token Lease last handed out for it (otherwise
	// ErrStaleLease; see invariant 6).
	DeadLetter(ctx context.Context, id string, token int64, cause error) error

	// Get returns a snapshot of the job identified by id, or ErrJobNotFound.
	Get(ctx context.Context, id string) (*Job, error)

	// List returns every job matching filter, ordered by CreatedAt ascending.
	List(ctx context.Context, filter Filter) ([]*Job, error)

	// QueueDepths returns the number of non-terminal jobs (StatePending,
	// StateLeased, StateWaiting) per queue.
	QueueDepths(ctx context.Context) (map[string]int, error)

	// Delete permanently removes id, regardless of its state.
	Delete(ctx context.Context, id string) error

	// Requeue resets id back to StatePending with Attempts cleared to zero and
	// AvailableAt set to now, regardless of its current state.
	Requeue(ctx context.Context, id string) error
}

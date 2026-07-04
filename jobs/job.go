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

// Package jobs provides at-least-once, crash-safe durable task execution on
// top of a GoAkt ActorSystem: due jobs are delivered as ordinary request/reply
// messages to actors or grains, with the handler's outcome driving
// acknowledgment, retry with configurable backoff, and dead-lettering. It also
// provides first-class fan-out/fan-in: a parent job can be split into many
// child jobs distributed across a cluster, whose results are aggregated back
// into a single reduce-step delivery to the parent's target once every child
// has completed.
//
// The Store interface (Enqueue/Lease/Ack/Nack/DeadLetter, plus the read/admin
// surface used by Inspector) is storage-agnostic: NewMemoryStore is the
// built-in default, and durable adapters (SQL, ...) can be implemented outside
// this package as long as they uphold the invariants documented on Store.
package jobs

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/goakt/v4/remote"
)

// State is the lifecycle state of a Job as tracked by a Store.
type State int

const (
	// StatePending means the job is enqueued and due at or after AvailableAt,
	// not currently leased.
	StatePending State = iota
	// StateLeased means the job was handed out by Lease to a worker and is
	// awaiting Ack/Nack before LeaseExpiresAt.
	StateLeased
	// StateWaiting means the job is a fan-out parent waiting on its outstanding
	// children to complete; it is never returned by Lease.
	StateWaiting
	// StateSucceeded is terminal: the job was acknowledged by its handler.
	StateSucceeded
	// StateDeadLetter is terminal: retries were exhausted, or the job was
	// explicitly dead-lettered, or (for a fan-out parent) one of its children
	// was dead-lettered.
	StateDeadLetter
)

// String implements fmt.Stringer.
func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateLeased:
		return "leased"
	case StateWaiting:
		return "waiting"
	case StateSucceeded:
		return "succeeded"
	case StateDeadLetter:
		return "dead_letter"
	default:
		return "unknown"
	}
}

// ChildResult is one fan-out child's outcome, recorded against its parent by a
// Store once the child reaches a terminal state. A parent job's ChildResults
// slice is populated, ordered by ChildIndex, once every child has completed
// and the parent transitions back to StatePending for its reduce-step
// delivery.
type ChildResult struct {
	ChildIndex int
	Payload    []byte
	Failed     bool
	Error      string
}

// Job is a single unit of durable work tracked by a Store.
//
// Payload carries the application message already serialized through
// remote.ProtoSerializer, exactly as NewJob leaves it: Store implementations
// never need to know about proto.Message, only about opaque bytes.
type Job struct {
	ID          string
	Queue       string
	TargetName  string
	Payload     []byte
	RetryPolicy RetryPolicy

	Attempts       int
	State          State
	AvailableAt    time.Time
	LeaseExpiresAt time.Time
	LeaseOwner     string
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time

	// Fan-out/fan-in linkage. ParentID and ChildIndex are set only on a
	// fan-out child (created via EnqueueFanOut); ChildCount and ChildResults
	// are set only on its parent.
	ParentID     string
	ChildIndex   int
	ChildCount   int
	ChildResults []ChildResult
}

// Clone returns a deep-enough copy of j: Store implementations hand out clones
// so callers cannot mutate internal state by mutating a returned *Job.
func (j *Job) Clone() *Job {
	clone := *j
	if j.Payload != nil {
		clone.Payload = append([]byte(nil), j.Payload...)
	}
	if j.ChildResults != nil {
		clone.ChildResults = make([]ChildResult, len(j.ChildResults))
		for i, r := range j.ChildResults {
			if r.Payload != nil {
				r.Payload = append([]byte(nil), r.Payload...)
			}
			clone.ChildResults[i] = r
		}
	}
	return &clone
}

// JobOption configures a Job built by NewJob.
type JobOption func(*Job)

// WithRetryPolicy overrides the default retry policy (DefaultRetryPolicy) for
// the job being built.
func WithRetryPolicy(policy RetryPolicy) JobOption {
	return func(j *Job) {
		j.RetryPolicy = policy
	}
}

// WithDelay makes the job due only after d has elapsed, instead of being
// immediately available.
func WithDelay(d time.Duration) JobOption {
	return func(j *Job) {
		j.AvailableAt = j.CreatedAt.Add(d)
	}
}

// WithID overrides the auto-generated job id. Passing a caller-chosen,
// deterministic id turns Enqueue into an idempotent operation: a Store rejects
// a duplicate id with ErrJobAlreadyExists.
func WithID(id string) JobOption {
	return func(j *Job) {
		j.ID = id
	}
}

// NewJob builds a Job ready for Store.Enqueue (or EnqueueFanOut): it
// serializes message through the same remoting pipeline used for RemoteTell
// (remote.ProtoSerializer), and fills in defaults: a random id,
// DefaultRetryPolicy, StatePending, and timestamps set to now.
func NewJob(queue, targetName string, message proto.Message, opts ...JobOption) (*Job, error) {
	if queue == "" {
		return nil, ErrInvalidQueue
	}
	if targetName == "" {
		return nil, ErrInvalidTargetName
	}
	if message == nil {
		return nil, ErrPayloadNotProto
	}

	payload, err := remote.NewProtoSerializer().Serialize(message)
	if err != nil {
		return nil, fmt.Errorf("jobs: failed to serialize payload: %w", err)
	}

	now := time.Now()
	job := &Job{
		ID:          uuid.NewString(),
		Queue:       queue,
		TargetName:  targetName,
		Payload:     payload,
		RetryPolicy: DefaultRetryPolicy(),
		State:       StatePending,
		AvailableAt: now,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	for _, opt := range opts {
		opt(job)
	}

	if err := job.RetryPolicy.validate(); err != nil {
		return nil, err
	}

	return job, nil
}

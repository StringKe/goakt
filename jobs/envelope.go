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
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tochemey/goakt/v4/internal/internalpb"
)

// jobStateToPB and its inverse translate between the Go-ergonomic State and
// the wire enum internalpb.JobState, so a durable Store adapter can persist a
// Job as an internalpb.JobEnvelope without depending on the Go State's iota
// ordering being wire-stable.
var jobStateToPB = map[State]internalpb.JobState{
	StatePending:    internalpb.JobState_JOB_STATE_PENDING,
	StateLeased:     internalpb.JobState_JOB_STATE_LEASED,
	StateWaiting:    internalpb.JobState_JOB_STATE_WAITING,
	StateSucceeded:  internalpb.JobState_JOB_STATE_SUCCEEDED,
	StateDeadLetter: internalpb.JobState_JOB_STATE_DEAD_LETTER,
}

var pbToJobState = map[internalpb.JobState]State{
	internalpb.JobState_JOB_STATE_PENDING:     StatePending,
	internalpb.JobState_JOB_STATE_LEASED:      StateLeased,
	internalpb.JobState_JOB_STATE_WAITING:     StateWaiting,
	internalpb.JobState_JOB_STATE_SUCCEEDED:   StateSucceeded,
	internalpb.JobState_JOB_STATE_DEAD_LETTER: StateDeadLetter,
}

var backoffKindToPB = map[BackoffKind]internalpb.BackoffKind{
	BackoffFixed:       internalpb.BackoffKind_BACKOFF_KIND_FIXED,
	BackoffLinear:      internalpb.BackoffKind_BACKOFF_KIND_LINEAR,
	BackoffExponential: internalpb.BackoffKind_BACKOFF_KIND_EXPONENTIAL,
}

var pbToBackoffKind = map[internalpb.BackoffKind]BackoffKind{
	internalpb.BackoffKind_BACKOFF_KIND_FIXED:       BackoffFixed,
	internalpb.BackoffKind_BACKOFF_KIND_LINEAR:      BackoffLinear,
	internalpb.BackoffKind_BACKOFF_KIND_EXPONENTIAL: BackoffExponential,
}

// ToEnvelope converts j into its wire/durable representation. Durable Store
// adapters (SQL, ...) use this, together with FromEnvelope, to persist a Job
// as an opaque, versionable protobuf message instead of depending on this
// package's internal Go layout.
//
// ChildResults is intentionally not part of internalpb.JobEnvelope: an
// adapter that needs to persist a waiting parent's partial fan-in state
// should store its children's JobEnvelope rows and recompute aggregation from
// them, the same way NewMemoryStore does in memory.
func (j *Job) ToEnvelope() *internalpb.JobEnvelope {
	envelope := &internalpb.JobEnvelope{
		Id:         j.ID,
		Queue:      j.Queue,
		TargetName: j.TargetName,
		Payload:    j.Payload,
		RetryPolicy: &internalpb.RetryPolicy{
			Kind:        backoffKindToPB[j.RetryPolicy.Kind],
			Base:        durationpb.New(j.RetryPolicy.Base),
			MaxDelay:    durationpb.New(j.RetryPolicy.MaxDelay),
			MaxAttempts: int32(j.RetryPolicy.MaxAttempts),
		},
		Attempts:       int32(j.Attempts),
		State:          jobStateToPB[j.State],
		AvailableAt:    timestamppb.New(j.AvailableAt),
		LeaseExpiresAt: timestamppb.New(j.LeaseExpiresAt),
		LeaseOwner:     j.LeaseOwner,
		LastError:      j.LastError,
		CreatedAt:      timestamppb.New(j.CreatedAt),
		UpdatedAt:      timestamppb.New(j.UpdatedAt),
		ParentId:       j.ParentID,
		ChildIndex:     int32(j.ChildIndex),
		ChildCount:     int32(j.ChildCount),
	}
	return envelope
}

// FromEnvelope reconstructs a Job from its wire/durable representation
// produced by ToEnvelope.
func FromEnvelope(envelope *internalpb.JobEnvelope) *Job {
	policy := RetryPolicy{}
	if rp := envelope.GetRetryPolicy(); rp != nil {
		policy = RetryPolicy{
			Kind:        pbToBackoffKind[rp.GetKind()],
			Base:        rp.GetBase().AsDuration(),
			MaxDelay:    rp.GetMaxDelay().AsDuration(),
			MaxAttempts: int(rp.GetMaxAttempts()),
		}
	}

	return &Job{
		ID:             envelope.GetId(),
		Queue:          envelope.GetQueue(),
		TargetName:     envelope.GetTargetName(),
		Payload:        envelope.GetPayload(),
		RetryPolicy:    policy,
		Attempts:       int(envelope.GetAttempts()),
		State:          pbToJobState[envelope.GetState()],
		AvailableAt:    asTime(envelope.GetAvailableAt()),
		LeaseExpiresAt: asTime(envelope.GetLeaseExpiresAt()),
		LeaseOwner:     envelope.GetLeaseOwner(),
		LastError:      envelope.GetLastError(),
		CreatedAt:      asTime(envelope.GetCreatedAt()),
		UpdatedAt:      asTime(envelope.GetUpdatedAt()),
		ParentID:       envelope.GetParentId(),
		ChildIndex:     int(envelope.GetChildIndex()),
		ChildCount:     int(envelope.GetChildCount()),
	}
}

func asTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

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

package actor

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"google.golang.org/protobuf/proto"

	gerrors "github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/internal/internalpb"
	"github.com/tochemey/goakt/v4/remote"
)

// scheduledMessageDescPrefix tags the Description() string produced for jobs
// created while a persistent scheduler JobQueue is configured, so
// DecodeScheduledMessage can recognize and reject unrelated input.
const scheduledMessageDescPrefix = "GoAktScheduledMessage"

// scheduledMessageJob is the quartz.Job implementation used by the scheduler
// when a persistent JobQueue is configured (see WithSchedulerJobQueue). Unlike
// the default job.FunctionJob closure, its state is entirely data-carrying: an
// envelope describing where to deliver the message and how to reconstruct it,
// so a persistent JobQueue can serialize it via Description() and reconstruct
// an equivalent, executable job after a process restart via
// NewScheduledMessageJob.
type scheduledMessageJob struct {
	system   ActorSystem
	envelope *internalpb.ScheduledMessage
}

var _ quartz.Job = (*scheduledMessageJob)(nil)

// NewScheduledMessageJob reconstructs an executable quartz.Job from a
// ScheduledMessage envelope previously produced by a persistent-queue-backed
// schedule (see WithSchedulerJobQueue). Custom JobQueue implementations that
// persist and later restore scheduled jobs (e.g. a SQL- or Redis-backed queue)
// use this, together with DecodeScheduledMessage and ScheduledMessageTrigger, to
// turn stored bytes back into a full quartz.ScheduledJob after a restart.
//
// At fire time, the target actor is looked up by name via system.ActorOf, so
// the message is delivered to whichever PID currently answers to that name;
// the actor does not need to be the same process instance that existed when
// the schedule was created.
func NewScheduledMessageJob(system ActorSystem, envelope *internalpb.ScheduledMessage) quartz.Job {
	return &scheduledMessageJob{system: system, envelope: envelope}
}

// Description returns a string encoding of the job's ScheduledMessage envelope.
// A persistent JobQueue implementation can treat this as an opaque, storable
// string and hand it back to DecodeScheduledMessage to reconstruct the envelope,
// without needing to understand GoAkt's internal types.
func (j *scheduledMessageJob) Description() string {
	return encodeScheduledMessageDesc(j.envelope)
}

// Execute resolves the target actor by name, decodes the payload, and delivers
// the message through the actor system.
func (j *scheduledMessageJob) Execute(ctx context.Context) error {
	envelope := j.envelope

	if envelope.GetClusterSingleFire() {
		// the TTL is re-derived from the persisted trigger, mirroring registration
		ttl := minScheduleFireClaimTTL
		if trigger, err := ScheduledMessageTrigger(envelope); err == nil {
			ttl = cronClaimTTL(trigger)
		}
		won, err := claimScheduleFireTick(ctx, j.system, envelope.GetReference(), ttl)
		if err != nil {
			return fmt.Errorf("failed to claim single-fire tick for reference=%s: %w", envelope.GetReference(), err)
		}
		if !won {
			return nil
		}
	}

	target, err := j.system.ActorOf(ctx, envelope.GetTargetName())
	if err != nil {
		return fmt.Errorf("failed to resolve scheduled message target=%s: %w", envelope.GetTargetName(), err)
	}

	decoded, err := remote.NewProtoSerializer().Deserialize(envelope.GetPayload())
	if err != nil {
		return fmt.Errorf("failed to deserialize scheduled message payload: %w", err)
	}

	message, ok := decoded.(proto.Message)
	if !ok {
		return gerrors.ErrScheduledMessageNotProto
	}

	sender := j.system.NoSender()
	if name := envelope.GetSenderName(); name != "" {
		if resolved, err := j.system.ActorOf(ctx, name); err == nil {
			sender = resolved
		}
		// A sender that can no longer be resolved (e.g. it was not respawned
		// after a restart) falls back to NoSender rather than failing delivery:
		// the sender is only used for tracing/reply-piping, not for correctness
		// of the scheduled delivery itself.
	}

	return sender.Tell(ctx, target, message)
}

// encodeScheduledMessageDesc marshals envelope into the Description() string
// format produced by scheduledMessageJob.
func encodeScheduledMessageDesc(envelope *internalpb.ScheduledMessage) string {
	data, err := proto.Marshal(envelope)
	if err != nil {
		// Description must not fail; fall back to a non-decodable but still
		// unique-ish description carrying the reference for diagnostics.
		return scheduledMessageDescPrefix + quartz.Sep + envelope.GetReference()
	}
	return scheduledMessageDescPrefix + quartz.Sep + base64.StdEncoding.EncodeToString(data)
}

// DecodeScheduledMessage decodes the Description() string produced by a job
// created through NewScheduledMessageJob back into its ScheduledMessage
// envelope, so a persistent JobQueue implementation can round-trip it.
//
// Returns ErrInvalidScheduledMessage when description was not produced by a
// scheduledMessageJob, or when the encoded envelope is corrupt.
func DecodeScheduledMessage(description string) (*internalpb.ScheduledMessage, error) {
	prefix := scheduledMessageDescPrefix + quartz.Sep
	encoded, found := strings.CutPrefix(description, prefix)
	if !found {
		return nil, gerrors.ErrInvalidScheduledMessage
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", gerrors.ErrInvalidScheduledMessage, err)
	}

	envelope := &internalpb.ScheduledMessage{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, fmt.Errorf("%w: %v", gerrors.ErrInvalidScheduledMessage, err)
	}
	return envelope, nil
}

// ScheduledMessageTrigger reconstructs the go-quartz Trigger described by
// envelope's trigger spec (once/interval/cron+timezone). Persistent JobQueue
// implementations use this, together with NewScheduledMessageJob, to rebuild a
// full quartz.ScheduledJob (Job + Trigger + NextRunTime) from durable storage.
//
// The reconstructed RunOnceTrigger is always marked Expired: this mirrors the
// state a RunOnceTrigger is already in immediately after the original
// ScheduleJob call, which computes its (one and only) fire time eagerly. The
// job's actual next-run time is tracked independently by the JobQueue
// implementation (as quartz.ScheduledJob.NextRunTime), not by this Trigger.
func ScheduledMessageTrigger(envelope *internalpb.ScheduledMessage) (quartz.Trigger, error) {
	spec := envelope.GetTrigger()
	switch kind := spec.GetKind().(type) {
	case *internalpb.ScheduleTrigger_Once:
		return &quartz.RunOnceTrigger{Delay: kind.Once.GetDelay().AsDuration(), Expired: true}, nil
	case *internalpb.ScheduleTrigger_Interval:
		return quartz.NewSimpleTrigger(kind.Interval.GetInterval().AsDuration()), nil
	case *internalpb.ScheduleTrigger_Cron:
		loc, err := time.LoadLocation(kind.Cron.GetTimezone())
		if err != nil {
			loc = time.UTC
		}
		return quartz.NewCronTriggerWithLoc(kind.Cron.GetExpression(), loc)
	default:
		return nil, gerrors.ErrInvalidScheduledMessage
	}
}

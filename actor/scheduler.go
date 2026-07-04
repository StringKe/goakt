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
	"fmt"
	"sync"
	"time"

	"github.com/reugn/go-quartz/job"
	quartzlogger "github.com/reugn/go-quartz/logger"
	"github.com/reugn/go-quartz/quartz"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/internal/internalpb"
	"github.com/tochemey/goakt/v4/internal/xsync"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
)

// scheduler defines the Go-Akt scheduler.
// Its job is to help stack messages that will be delivered in the future to actors.
type scheduler struct {
	// helps lock concurrent access
	mu sync.Mutex
	// underlying Scheduler
	quartzScheduler quartz.Scheduler
	// states whether the quartzScheduler has started or not
	started *atomic.Bool
	// define the logger
	logger log.Logger
	// define the shutdown timeout
	shutdownTimeout time.Duration
	// specifies the job keys mapping
	scheduledKeys *xsync.Map[string, *quartz.JobKey]
	// specifies the introspection metadata mapping used by ListSchedules
	scheduledMeta *xsync.Map[string, *scheduleMeta]
	// actorSystem is needed to resolve NoSender() for remote-PID schedules,
	// since remote PIDs carry no actor-system reference.
	actorSystem ActorSystem
	// persistent is true when jobQueue was supplied via WithSchedulerJobQueue.
	// It switches ScheduleOnce/Schedule/ScheduleWithCron from wrapping a
	// non-serializable closure to persisting a ScheduledMessage envelope, and
	// makes Start rebuild scheduledKeys from whatever the queue already holds.
	persistent bool
}

// newScheduler creates an instance of scheduler. jobQueue and queueLocker are
// optional (see WithSchedulerJobQueue); when either is nil the scheduler keeps
// go-quartz's default in-memory queue and behaves exactly as before this
// option existed.
func newScheduler(logger log.Logger, shutdownTimeout time.Duration, system ActorSystem, jobQueue quartz.JobQueue, queueLocker sync.Locker) *scheduler {
	// create an instance of quartz scheduler with logger off
	// Set a high OutdatedThreshold to prevent RunOnceTrigger jobs from being
	// silently dropped when they become "outdated" (scheduled time passed).
	// The default 100ms threshold causes issues because:
	// 1. RunOnceTrigger marks itself as expired during initial scheduling
	// 2. If the job becomes outdated, go-quartz tries to reschedule it
	// 3. The already-expired trigger returns an error, causing the job to be dropped
	// By setting a 24-hour threshold, we ensure jobs are executed even if delayed.
	opts := []quartz.SchedulerOpt{
		quartz.WithLogger(quartzlogger.NewSimpleLogger(nil, quartzlogger.LevelOff)),
		quartz.WithOutdatedThreshold(24 * time.Hour),
	}

	persistent := jobQueue != nil && queueLocker != nil
	if persistent {
		opts = append(opts, quartz.WithQueue(jobQueue, queueLocker))
	}

	quartzScheduler, _ := quartz.NewStdScheduler(opts...)

	// create an instance of scheduler
	scheduler := &scheduler{
		mu:              sync.Mutex{},
		started:         atomic.NewBool(false),
		quartzScheduler: quartzScheduler,
		logger:          logger,
		shutdownTimeout: shutdownTimeout,
		scheduledKeys:   xsync.NewMap[string, *quartz.JobKey](),
		scheduledMeta:   xsync.NewMap[string, *scheduleMeta](),
		actorSystem:     system,
		persistent:      persistent,
	}

	// return the instance of the scheduler
	return scheduler
}

// Start starts the scheduler
func (x *scheduler) Start(ctx context.Context) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.logger.Info("starting messages scheduler...")
	x.quartzScheduler.Start(ctx)
	x.started.Store(x.quartzScheduler.IsStarted())
	if x.persistent {
		x.rebuildScheduledKeys()
	}
	x.logger.Info("messages scheduler started.:)")
}

// rebuildScheduledKeys repopulates scheduledKeys from whatever the persistent
// JobQueue already holds. This is what lets schedules created by a previous
// process instance (and restored into the queue by its implementation) become
// manageable again via CancelSchedule/PauseSchedule/ResumeSchedule after a
// restart.
func (x *scheduler) rebuildScheduledKeys() {
	jobKeys, err := x.quartzScheduler.GetJobKeys()
	if err != nil {
		x.logger.Error(fmt.Errorf("failed to list persisted scheduled jobs: %w", err))
		return
	}

	for _, jobKey := range jobKeys {
		scheduledJob, err := x.quartzScheduler.GetScheduledJob(jobKey)
		if err != nil {
			x.logger.Error(fmt.Errorf("failed to load persisted job key=%s: %w", jobKey.String(), err))
			continue
		}

		msgJob, ok := scheduledJob.JobDetail().Job().(*scheduledMessageJob)
		if !ok {
			continue
		}

		x.scheduledKeys.Set(msgJob.envelope.GetReference(), jobKey)
	}
}

// Stop stops the scheduler
func (x *scheduler) Stop(ctx context.Context) {
	if !x.started.Load() {
		return
	}

	x.logger.Info("stopping messages scheduler...")
	x.mu.Lock()
	defer x.mu.Unlock()
	// With a persistent JobQueue, pending jobs must survive Stop so Start can
	// rebuild scheduledKeys from them again; Clear would erase the whole point
	// of configuring a durable queue. With the default in-memory queue, Clear
	// keeps prior behavior of dropping everything on Stop.
	if !x.persistent {
		_ = x.quartzScheduler.Clear()
	}
	x.quartzScheduler.Stop()
	x.started.Store(x.quartzScheduler.IsStarted())

	ctx, cancel := context.WithTimeout(ctx, x.shutdownTimeout)
	defer cancel()
	x.quartzScheduler.Wait(ctx)

	x.scheduledKeys.Reset()
	x.scheduledMeta.Reset()
	x.logger.Info("messages scheduler stopped...:)")
}

// ScheduleOnce schedules a one-time delivery of a message to the specified actor (PID) after a given delay.
//
// The message will be sent exactly once to the target actor after the specified duration has elapsed.
// This is a fire-and-forget scheduling mechanism — once delivered, the message will not be retried or repeated.
//
// Parameters:
//   - message: The proto.Message to be sent.
//   - pid: The PID of the actor that will receive the message.
//   - delay: The duration to wait before delivering the message.
//   - opts: Optional ScheduleOption values such as WithReference to control scheduling behavior.
//
// Returns:
//   - error: An error is returned if scheduling fails due to invalid input or internal errors.
//
// Note:
//   - It's strongly recommended to set a unique reference ID using WithReference if you intend to cancel, pause, or resume the message later.
//   - If no reference is set, an automatic one will be generated, which may not be easily retrievable.
func (x *scheduler) ScheduleOnce(message any, to *PID, delay time.Duration, opts ...ScheduleOption) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if !x.started.Load() {
		return errors.ErrSchedulerNotStarted
	}

	if to.IsRemote() && !to.remotingEnabled() {
		return errors.ErrRemotingDisabled
	}

	senderConfig := newScheduleConfig(opts...)
	reference := senderConfig.Reference()
	jobKey := quartz.NewJobKey(reference)
	x.recordSchedule(reference, &scheduleMeta{kind: TriggerKindOnce, interval: delay, address: to.Path().String()})

	triggerSpec := &internalpb.ScheduleTrigger{
		Kind: &internalpb.ScheduleTrigger_Once{Once: &internalpb.OnceTrigger{Delay: durationpb.New(delay)}},
	}
	detail, err := x.buildJobDetail(to, message, senderConfig, jobKey, triggerSpec)
	if err != nil {
		return err
	}

	x.scheduledKeys.Set(reference, jobKey)
	return x.quartzScheduler.ScheduleJob(detail, quartz.NewRunOnceTrigger(delay))
}

// Schedule schedules a recurring message to be delivered to the specified actor (PID) at a fixed interval.
//
// This function sets up a message to be sent repeatedly to the target actor, with each delivery occurring
// after the specified interval. The scheduling continues until explicitly canceled or if the actor is no longer available.
//
// Parameters:
//   - message: The proto.Message to be delivered at regular intervals.
//   - pid: The PID of the actor that will receive the message.
//   - interval: The time duration between each delivery of the message.
//   - opts: Optional ScheduleOption values such as WithReference to control scheduling behavior.
//
// Returns:
//   - error: An error is returned if the message could not be scheduled due to invalid input or internal issues.
//
// Note:
//   - It's strongly recommended to set a unique reference ID using WithReference if you plan to cancel, pause, or resume the scheduled message.
//   - If no reference is set, an automatic one will be generated internally, which may not be easily retrievable for later operations.
//   - This function does not provide built-in delivery guarantees such as at-least-once or exactly-once semantics; ensure idempotency where needed.
func (x *scheduler) Schedule(message any, to *PID, interval time.Duration, opts ...ScheduleOption) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if !x.started.Load() {
		return errors.ErrSchedulerNotStarted
	}

	if to.IsRemote() && !to.remotingEnabled() {
		return errors.ErrRemotingDisabled
	}

	senderConfig := newScheduleConfig(opts...)
	reference := senderConfig.Reference()
	jobKey := quartz.NewJobKey(reference)
	x.recordSchedule(reference, &scheduleMeta{kind: TriggerKindInterval, interval: interval, address: to.Path().String()})

	triggerSpec := &internalpb.ScheduleTrigger{
		Kind: &internalpb.ScheduleTrigger_Interval{Interval: &internalpb.IntervalTrigger{Interval: durationpb.New(interval)}},
	}
	detail, err := x.buildJobDetail(to, message, senderConfig, jobKey, triggerSpec)
	if err != nil {
		return err
	}

	x.scheduledKeys.Set(reference, jobKey)
	return x.quartzScheduler.ScheduleJob(detail, quartz.NewSimpleTrigger(interval))
}

// ScheduleWithCron schedules a message to be delivered to the specified actor (PID) using a cron expression.
//
// This method enables flexible time-based scheduling using standard cron syntax, allowing you to specify complex recurring schedules.
// The message will be sent to the target actor according to the schedule defined by the cron expression.
//
// Parameters:
//   - message: The proto.Message to be delivered.
//   - pid: The PID of the actor that will receive the message.
//   - cronExpression: A standard cron-formatted string (e.g., "0 */5 * * * *") representing the schedule.
//   - opts: Optional ScheduleOption values such as WithReference to control scheduling behavior.
//
// Returns:
//   - error: An error is returned if the cron expression is invalid or if scheduling fails due to internal errors.
//
// Note:
//   - It's strongly recommended to set a unique reference ID using WithReference if you plan to cancel, pause, or resume the scheduled message.
//   - If no reference is set, an automatic one will be generated internally, which may not be easily retrievable for future operations.
//   - The cron expression must follow the format supported by the scheduler (typically 6 or 5 fields depending on implementation).
func (x *scheduler) ScheduleWithCron(message any, to *PID, cronExpression string, opts ...ScheduleOption) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if !x.started.Load() {
		return errors.ErrSchedulerNotStarted
	}

	if to.IsRemote() && !to.remotingEnabled() {
		return errors.ErrRemotingDisabled
	}

	senderConfig := newScheduleConfig(opts...)
	reference := senderConfig.Reference()
	jobKey := quartz.NewJobKey(reference)

	location := time.Now().Location()
	trigger, err := quartz.NewCronTriggerWithLoc(cronExpression, location)
	if err != nil {
		x.logger.Error(fmt.Errorf("failed to schedule message: %w", err))
		return err
	}
	x.recordSchedule(reference, &scheduleMeta{kind: TriggerKindCron, expression: cronExpression, address: to.Path().String()})

	triggerSpec := &internalpb.ScheduleTrigger{
		Kind: &internalpb.ScheduleTrigger_Cron{Cron: &internalpb.CronTrigger{Expression: cronExpression, Timezone: location.String()}},
	}
	detail, err := x.buildJobDetail(to, message, senderConfig, jobKey, triggerSpec)
	if err != nil {
		return err
	}

	x.scheduledKeys.Set(reference, jobKey)
	return x.quartzScheduler.ScheduleJob(detail, trigger)
}

// CancelSchedule cancels a previously scheduled message intended for delivery to a target actor (PID).
//
// It attempts to locate and cancel the scheduled task associated with the specified message reference.
// If the scheduled message cannot be found, has already been delivered, or was previously canceled, an error is returned.
//
// Parameters:
//   - reference: The message reference previously used when scheduling the message
//
// Returns:
//   - error: An error is returned if the scheduled message could not be found or canceled.
func (x *scheduler) CancelSchedule(reference string) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	defer x.scheduledKeys.Delete(reference)
	defer x.scheduledMeta.Delete(reference)

	if !x.started.Load() {
		return errors.ErrSchedulerNotStarted
	}

	jobKey, ok := x.scheduledKeys.Get(reference)
	if !ok {
		return errors.ErrScheduledReferenceNotFound
	}

	return x.quartzScheduler.DeleteJob(jobKey)
}

// PauseSchedule pauses a previously scheduled message that was set to be delivered to a target actor (PID).
//
// This function temporarily halts the delivery of the scheduled message. It can be resumed later using a corresponding resume mechanism,
// depending on the scheduler's capabilities. If the message has already been delivered or cannot be found, an error is returned.
//
// Parameters:
//   - reference: The message reference previously used when scheduling the message
//
// Returns:
//   - error: An error is returned if the scheduled message cannot be found, has already been delivered, or cannot be paused.
func (x *scheduler) PauseSchedule(reference string) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if !x.started.Load() {
		return errors.ErrSchedulerNotStarted
	}

	jobKey, ok := x.scheduledKeys.Get(reference)
	if !ok {
		return errors.ErrScheduledReferenceNotFound
	}

	return x.quartzScheduler.PauseJob(jobKey)
}

// ResumeSchedule resumes a previously paused scheduled message intended for delivery to a target actor (PID).
//
// This function reactivates a scheduled message that was previously paused, allowing it to continue toward delivery.
// If the message has already been delivered, canceled, or cannot be found, an error is returned.
//
// Parameters:
//   - reference: The message reference previously used when scheduling the message
//
// Returns:
//   - error: An error is returned if the scheduled message cannot be found, was never paused, has already been delivered, or cannot be resumed.
func (x *scheduler) ResumeSchedule(reference string) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if !x.started.Load() {
		return errors.ErrSchedulerNotStarted
	}

	jobKey, ok := x.scheduledKeys.Get(reference)
	if !ok {
		return errors.ErrScheduledReferenceNotFound
	}

	return x.quartzScheduler.ResumeJob(jobKey)
}

// makeJobFn returns the job function for a scheduled delivery.
// PID.Tell is already location-transparent (local and remote), so no IsRemote check is needed here.
// The scheduler holds its own actorSystem reference so that NoSender() can be resolved even
// when `to` is a remote PID (remote PIDs carry no ActorSystem reference).
func (x *scheduler) makeJobFn(to *PID, message any, cfg *scheduleConfig) func(ctx context.Context) (bool, error) {
	noSender := x.actorSystem.NoSender()
	sender := cfg.Sender()
	if sender == nil || sender.Equals(noSender) {
		sender = noSender
	}
	return func(ctx context.Context) (bool, error) {
		err := sender.Tell(ctx, to, message)
		return err == nil, err
	}
}

// buildJobDetail returns the quartz.JobDetail to schedule for to/message/cfg.
// When no persistent JobQueue is configured, it wraps a plain closure exactly
// as before this option existed. When a persistent JobQueue is configured, it
// instead builds a ScheduledMessage envelope and wraps it in a
// scheduledMessageJob, so the job can be persisted and rebuilt after a restart.
func (x *scheduler) buildJobDetail(to *PID, message any, cfg *scheduleConfig, jobKey *quartz.JobKey, triggerSpec *internalpb.ScheduleTrigger) (*quartz.JobDetail, error) {
	if !x.persistent {
		jobFn := x.makeJobFn(to, message, cfg)
		return quartz.NewJobDetail(job.NewFunctionJob(jobFn), jobKey), nil
	}

	envelope, err := x.buildEnvelope(to, message, cfg, triggerSpec)
	if err != nil {
		return nil, err
	}
	return quartz.NewJobDetail(NewScheduledMessageJob(x.actorSystem, envelope), jobKey), nil
}

// buildEnvelope serializes message through the same remoting pipeline used for
// RemoteTell and packages it, along with the target/sender names and trigger
// spec, into a ScheduledMessage envelope that a persistent JobQueue can store.
func (x *scheduler) buildEnvelope(to *PID, message any, cfg *scheduleConfig, triggerSpec *internalpb.ScheduleTrigger) (*internalpb.ScheduledMessage, error) {
	protoMessage, ok := message.(proto.Message)
	if !ok {
		return nil, errors.ErrScheduledMessageNotProto
	}

	payload, err := remote.NewProtoSerializer().Serialize(protoMessage)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize scheduled message: %w", err)
	}

	envelope := &internalpb.ScheduledMessage{
		Reference:  cfg.Reference(),
		TargetName: to.Name(),
		Payload:    payload,
		Trigger:    triggerSpec,
	}

	noSender := x.actorSystem.NoSender()
	if sender := cfg.Sender(); sender != nil && !sender.Equals(noSender) {
		envelope.SenderName = sender.Name()
	}

	return envelope, nil
}

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
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/internal/internalpb"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
)

// FanInEnvelope is the message a fan-out parent's target receives once every
// child has completed: the aggregated results, ordered by child index. It is
// a public alias for the wire type the Engine actually delivers, so
// application code handling a parent's reduce step does not need to import
// internal/internalpb directly.
type FanInEnvelope = internalpb.FanInEnvelope

// JobResult is one fan-out child's outcome as carried by a FanInEnvelope. It
// is a public alias for the wire type; see ChildResult for the equivalent
// Go-ergonomic shape a Store hands back on Job.ChildResults.
type JobResult = internalpb.JobResult

// Default tuning values applied by EngineConfig when left unset.
const (
	DefaultPollInterval = 250 * time.Millisecond
	DefaultLeaseTTL     = 30 * time.Second
	DefaultBatchSize    = 32
	DefaultConcurrency  = 8
)

// EngineConfig configures an Engine.
type EngineConfig struct {
	// Store persists jobs. Required.
	Store Store
	// Queues restricts polling to the given queue names. Empty means every
	// queue.
	Queues []string
	// PollInterval is how often the engine leases due jobs. Defaults to
	// DefaultPollInterval.
	PollInterval time.Duration
	// LeaseTTL bounds how long a leased job may run before another engine
	// instance is allowed to reclaim and redeliver it. Defaults to
	// DefaultLeaseTTL.
	LeaseTTL time.Duration
	// BatchSize caps how many jobs a single poll leases per queue. Defaults to
	// DefaultBatchSize.
	BatchSize int
	// AskTimeout bounds how long the engine waits for a target's handler to
	// reply before treating the delivery as failed. Defaults to LeaseTTL.
	AskTimeout time.Duration
	// Concurrency caps how many job deliveries run at once. Defaults to
	// DefaultConcurrency.
	Concurrency int
	// Logger receives operational errors (lease/ack/nack failures). Defaults
	// to log.DiscardLogger.
	Logger log.Logger
}

func (c *EngineConfig) setDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.AskTimeout <= 0 {
		c.AskTimeout = c.LeaseTTL
	}
	if c.Concurrency <= 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.Logger == nil {
		c.Logger = log.DiscardLogger
	}
}

// Engine binds a Store to an ActorSystem: it polls for due jobs, delivers
// each as a synchronous (Ask) message to its target actor or grain, and lets
// the handler's outcome drive Ack/Nack. A lease that is never Ack/Nack'd
// (because the worker holding it crashed) simply expires and is picked up
// again by Lease on a later poll, from this or any other Engine instance
// sharing the same Store -- delivery is crash-safe by construction.
type Engine struct {
	system actor.ActorSystem
	store  Store
	queues []string

	pollInterval time.Duration
	leaseTTL     time.Duration
	batchSize    int
	askTimeout   time.Duration

	owner string
	sem   chan struct{}
	logger log.Logger

	started *atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewEngine builds an Engine bound to system, using cfg.Store for
// persistence. The engine is not started until Start is called.
func NewEngine(system actor.ActorSystem, cfg EngineConfig) (*Engine, error) {
	if system == nil {
		return nil, ErrActorSystemRequired
	}
	if cfg.Store == nil {
		return nil, ErrStoreRequired
	}
	cfg.setDefaults()

	queues := cfg.Queues
	if len(queues) == 0 {
		queues = []string{""} // "" matches every queue in Store.Lease
	}

	return &Engine{
		system:       system,
		store:        cfg.Store,
		queues:       queues,
		pollInterval: cfg.PollInterval,
		leaseTTL:     cfg.LeaseTTL,
		batchSize:    cfg.BatchSize,
		askTimeout:   cfg.AskTimeout,
		owner:        uuid.NewString(),
		sem:          make(chan struct{}, cfg.Concurrency),
		logger:       cfg.Logger,
		started:      atomic.NewBool(false),
	}, nil
}

// Store returns the Store this engine was configured with.
func (e *Engine) Store() Store {
	return e.store
}

// Inspector returns an Inspector over this engine's Store.
func (e *Engine) Inspector() *Inspector {
	return NewInspector(e.store)
}

// Enqueue durably records job for future delivery. It is a convenience
// wrapper over Store.Enqueue.
func (e *Engine) Enqueue(ctx context.Context, job *Job) error {
	return e.store.Enqueue(ctx, job)
}

// FanOut splits parent into the given children, distributed across whichever
// cluster member's Engine next leases each one: results are aggregated back
// into parent once every child completes, and parent is then delivered to its
// own target as a *internalpb.FanInEnvelope carrying every child's result
// ordered by child index. It is a convenience wrapper over
// Store.EnqueueFanOut.
func (e *Engine) FanOut(ctx context.Context, parent *Job, children []*Job) error {
	return e.store.EnqueueFanOut(ctx, parent, children)
}

// Start begins polling the Store for due jobs on a background goroutine.
// Start returns immediately; delivery happens asynchronously until Stop is
// called.
func (e *Engine) Start(_ context.Context) error {
	if !e.started.CompareAndSwap(false, true) {
		return ErrEngineAlreadyStarted
	}

	runCtx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.wg.Add(1)
	go e.run(runCtx)
	return nil
}

// Stop halts polling and waits for in-flight deliveries to finish or ctx to
// expire, whichever comes first. Jobs already leased but not yet
// Ack/Nack'd when ctx expires remain leased until LeaseTTL passes, at which
// point any Engine sharing the Store will redeliver them.
func (e *Engine) Stop(ctx context.Context) error {
	if !e.started.CompareAndSwap(true, false) {
		return nil
	}
	e.cancel()

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) run(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.poll(ctx)
		}
	}
}

func (e *Engine) poll(ctx context.Context) {
	for _, queue := range e.queues {
		due, err := e.store.Lease(ctx, queue, e.owner, e.leaseTTL, e.batchSize)
		if err != nil {
			e.logger.Error(fmt.Errorf("jobs: lease failed queue=%q: %w", queue, err))
			continue
		}

		for _, job := range due {
			select {
			case e.sem <- struct{}{}:
			case <-ctx.Done():
				return
			}

			e.wg.Add(1)
			go func(job *Job) {
				defer e.wg.Done()
				defer func() { <-e.sem }()
				e.deliver(ctx, job)
			}(job)
		}
	}
}

// deliver decodes job's payload, resolves its target by name (local or
// cluster-wide, via ActorSystem.ActorOf), and sends it as a synchronous Ask.
// The handler's outcome drives Ack/Nack:
//
//   - A transport-level failure -- decode failure, unresolvable target, dead
//     actor, or an Ask timeout because the handler never replied -- nacks the
//     job for retry per its RetryPolicy.
//   - A handler that replies via ReceiveContext.Response with an error value
//     is treated the same way: this is the supported way for a handler to
//     reject a job explicitly (as opposed to a bug that hangs the handler
//     until the Ask times out).
//   - A handler that replies with anything else acknowledges the job,
//     propagating the response to a fan-out parent, if any.
func (e *Engine) deliver(ctx context.Context, job *Job) {
	message, err := e.decode(job)
	if err != nil {
		e.nack(ctx, job, err)
		return
	}

	target, err := e.system.ActorOf(ctx, job.TargetName)
	if err != nil {
		e.nack(ctx, job, err)
		return
	}

	response, err := e.system.NoSender().Ask(ctx, target, message, e.askTimeout)
	if err != nil {
		e.nack(ctx, job, err)
		return
	}
	if handlerErr, failed := response.(error); failed {
		e.nack(ctx, job, handlerErr)
		return
	}

	if ackErr := e.store.Ack(ctx, job.ID, e.encodeResult(response)); ackErr != nil {
		e.logger.Error(fmt.Errorf("jobs: ack failed job=%s: %w", job.ID, ackErr))
	}
}

func (e *Engine) nack(ctx context.Context, job *Job, cause error) {
	if err := e.store.Nack(ctx, job.ID, cause); err != nil {
		e.logger.Error(fmt.Errorf("jobs: nack failed job=%s: %w", job.ID, err))
	}
}

// decode builds the proto.Message to deliver for job. A fan-out parent whose
// children have all completed carries its aggregated ChildResults instead of
// a meaningful Payload, so it is delivered as a *internalpb.FanInEnvelope; a
// regular job or fan-out child is delivered as its decoded Payload. A
// StateWaiting parent is never leased (see Store's invariant 3), so there is
// no ambiguity between "not yet aggregated" and "aggregated".
func (e *Engine) decode(job *Job) (proto.Message, error) {
	if job.ChildCount > 0 {
		return buildFanInEnvelope(job), nil
	}

	decoded, err := remote.NewProtoSerializer().Deserialize(job.Payload)
	if err != nil {
		return nil, fmt.Errorf("jobs: failed to deserialize payload: %w", err)
	}
	message, ok := decoded.(proto.Message)
	if !ok {
		return nil, ErrPayloadNotProto
	}
	return message, nil
}

func buildFanInEnvelope(job *Job) *internalpb.FanInEnvelope {
	results := make([]*internalpb.JobResult, len(job.ChildResults))
	for i, r := range job.ChildResults {
		results[i] = &internalpb.JobResult{
			ChildIndex: int32(r.ChildIndex),
			Payload:    r.Payload,
			Failed:     r.Failed,
			Error:      r.Error,
		}
	}
	return &internalpb.FanInEnvelope{JobId: job.ID, Results: results}
}

// encodeResult serializes a handler's response for propagation to a fan-out
// parent via Ack. A nil or non-proto response yields a nil result, which is
// harmless: it is only ever consulted for fan-out children.
func (e *Engine) encodeResult(response any) []byte {
	message, ok := response.(proto.Message)
	if !ok || message == nil {
		return nil
	}

	payload, err := remote.NewProtoSerializer().Serialize(message)
	if err != nil {
		e.logger.Error(fmt.Errorf("jobs: failed to serialize handler response: %w", err))
		return nil
	}
	return payload
}

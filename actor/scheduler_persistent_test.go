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
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/internal/internalpb"
	dynaport "github.com/tochemey/goakt/v4/internal/net"
	"github.com/tochemey/goakt/v4/internal/pause"
	"github.com/tochemey/goakt/v4/log"
	testkit "github.com/tochemey/goakt/v4/mocks/discovery"
	"github.com/tochemey/goakt/v4/remote"
	"github.com/tochemey/goakt/v4/test/data/testpb"
)

// storedJob is what fakeJobQueue keeps for a job: exactly the bytes and
// bookkeeping an external, persistent store would keep. It never holds a
// reference to the live quartz.ScheduledJob it was given.
type storedJob struct {
	description string
	keyName     string
	keyGroup    string
	opts        *quartz.JobDetailOptions
	nextRunTime int64
}

// fakeJobQueue is a quartz.JobQueue test double standing in for an external,
// durable store (SQL, Redis, ...). It proves that GoAkt schedules created
// while a persistent queue is configured really are serializable: every read
// reconstructs a fresh quartz.ScheduledJob from stored bytes via
// DecodeScheduledMessage, ScheduledMessageTrigger, and NewScheduledMessageJob,
// exactly as a real adapter implementation would.
type fakeJobQueue struct {
	mu     sync.Mutex
	system ActorSystem
	jobs   map[string]*storedJob
}

func newFakeJobQueue(system ActorSystem) *fakeJobQueue {
	return &fakeJobQueue{system: system, jobs: make(map[string]*storedJob)}
}

var _ quartz.JobQueue = (*fakeJobQueue)(nil)

func (q *fakeJobQueue) Push(job quartz.ScheduledJob) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := job.JobDetail().JobKey().String()
	if _, exists := q.jobs[key]; exists && !job.JobDetail().Options().Replace {
		return quartz.ErrJobAlreadyExists
	}

	q.jobs[key] = &storedJob{
		description: job.JobDetail().Job().Description(),
		keyName:     job.JobDetail().JobKey().Name(),
		keyGroup:    job.JobDetail().JobKey().Group(),
		opts:        job.JobDetail().Options(),
		nextRunTime: job.NextRunTime(),
	}
	return nil
}

func (q *fakeJobQueue) Pop() (quartz.ScheduledJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key, stored := q.headLocked()
	if stored == nil {
		return nil, quartz.ErrQueueEmpty
	}
	delete(q.jobs, key)
	return q.reconstruct(stored)
}

func (q *fakeJobQueue) Head() (quartz.ScheduledJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	_, stored := q.headLocked()
	if stored == nil {
		return nil, quartz.ErrQueueEmpty
	}
	return q.reconstruct(stored)
}

func (q *fakeJobQueue) Get(jobKey *quartz.JobKey) (quartz.ScheduledJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stored, ok := q.jobs[jobKey.String()]
	if !ok {
		return nil, quartz.ErrJobNotFound
	}
	return q.reconstruct(stored)
}

func (q *fakeJobQueue) Remove(jobKey *quartz.JobKey) (quartz.ScheduledJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stored, ok := q.jobs[jobKey.String()]
	if !ok {
		return nil, quartz.ErrJobNotFound
	}
	delete(q.jobs, jobKey.String())
	return q.reconstruct(stored)
}

func (q *fakeJobQueue) ScheduledJobs(matchers []quartz.Matcher[quartz.ScheduledJob]) ([]quartz.ScheduledJob, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	jobs := make([]quartz.ScheduledJob, 0, len(q.jobs))
	for _, stored := range q.jobs {
		reconstructed, err := q.reconstruct(stored)
		if err != nil {
			return nil, err
		}

		matched := true
		for _, matcher := range matchers {
			if !matcher.IsMatch(reconstructed) {
				matched = false
				break
			}
		}
		if matched {
			jobs = append(jobs, reconstructed)
		}
	}
	return jobs, nil
}

func (q *fakeJobQueue) Size() (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs), nil
}

func (q *fakeJobQueue) Clear() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = make(map[string]*storedJob)
	return nil
}

// headLocked returns the stored job with the smallest nextRunTime. Callers
// must hold q.mu.
func (q *fakeJobQueue) headLocked() (string, *storedJob) {
	var bestKey string
	var best *storedJob
	for key, stored := range q.jobs {
		if best == nil || stored.nextRunTime < best.nextRunTime {
			bestKey, best = key, stored
		}
	}
	return bestKey, best
}

// reconstruct turns stored bytes back into an executable quartz.ScheduledJob,
// mirroring what a real persistent JobQueue implementation would do on load.
func (q *fakeJobQueue) reconstruct(stored *storedJob) (quartz.ScheduledJob, error) {
	envelope, err := DecodeScheduledMessage(stored.description)
	if err != nil {
		return nil, err
	}

	trigger, err := ScheduledMessageTrigger(envelope)
	if err != nil {
		return nil, err
	}

	jobKey := quartz.NewJobKeyWithGroup(stored.keyName, stored.keyGroup)
	jobDetail := quartz.NewJobDetailWithOptions(NewScheduledMessageJob(q.system, envelope), jobKey, stored.opts)

	return &fakeScheduledJob{jobDetail: jobDetail, trigger: trigger, nextRunTime: stored.nextRunTime}, nil
}

// fakeScheduledJob implements quartz.ScheduledJob over reconstructed state.
type fakeScheduledJob struct {
	jobDetail   *quartz.JobDetail
	trigger     quartz.Trigger
	nextRunTime int64
}

func (f *fakeScheduledJob) JobDetail() *quartz.JobDetail { return f.jobDetail }
func (f *fakeScheduledJob) Trigger() quartz.Trigger      { return f.trigger }
func (f *fakeScheduledJob) NextRunTime() int64           { return f.nextRunTime }

// newPersistentTestSystem creates and starts an actor system with a single
// spawned MockActor, for use by the persistent-scheduler tests below.
func newPersistentTestSystem(t *testing.T, systemName, actorName string) (ActorSystem, *PID) {
	t.Helper()
	ctx := context.TODO()

	sys, err := NewActorSystem(systemName, WithLogger(log.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, sys.Start(ctx))
	t.Cleanup(func() { _ = sys.Stop(context.TODO()) })

	pause.For(200 * time.Millisecond)

	pid, err := sys.Spawn(ctx, actorName, NewMockActor())
	require.NoError(t, err)

	pause.For(200 * time.Millisecond)
	return sys, pid
}

func TestSchedulerPersistentJobQueue(t *testing.T) {
	t.Run("ScheduleOnce survives a scheduler Stop/Start cycle", func(t *testing.T) {
		sys, pid := newPersistentTestSystem(t, "persist-once", "target")

		queue := newFakeJobQueue(sys)
		locker := new(sync.Mutex)
		sched := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		sched.Start(context.TODO())

		message := new(testpb.TestSend)
		require.NoError(t, sched.ScheduleOnce(message, pid, 400*time.Millisecond, WithReference("once-ref")))

		size, err := queue.Size()
		require.NoError(t, err)
		assert.Equal(t, 1, size)

		// Stop must not wipe the persistent queue.
		sched.Stop(context.TODO())
		size, err = queue.Size()
		require.NoError(t, err)
		assert.Equal(t, 1, size)

		// Simulate a process restart: a brand-new scheduler bound to the same
		// external queue must rebuild scheduledKeys and still deliver the job.
		restarted := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		restarted.Start(context.TODO())
		defer restarted.Stop(context.TODO())

		_, ok := restarted.scheduledKeys.Get("once-ref")
		assert.True(t, ok)

		pause.For(700 * time.Millisecond)
		assert.EqualValues(t, 1, pid.ProcessedCount()-1)
	})

	t.Run("ListSchedules reports schedules rebuilt from the persistent queue", func(t *testing.T) {
		sys, pid := newPersistentTestSystem(t, "persist-introspect", "target")

		queue := newFakeJobQueue(sys)
		locker := new(sync.Mutex)
		sched := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		sched.Start(context.TODO())

		message := new(testpb.TestSend)
		require.NoError(t, sched.ScheduleOnce(message, pid, time.Hour, WithReference("intro-once")))
		require.NoError(t, sched.Schedule(message, pid, time.Hour, WithReference("intro-interval")))
		require.NoError(t, sched.ScheduleWithCron(message, pid, "0 0 * * * *", WithReference("intro-cron")))

		sched.Stop(context.TODO())

		restarted := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		restarted.Start(context.TODO())
		defer restarted.Stop(context.TODO())

		infos := restarted.ListSchedules()
		byRef := make(map[string]ScheduleInfo, len(infos))
		for _, info := range infos {
			byRef[info.Reference] = info
		}

		require.Contains(t, byRef, "intro-once")
		assert.Equal(t, TriggerKindOnce, byRef["intro-once"].TriggerKind)
		assert.Equal(t, time.Hour, byRef["intro-once"].Interval)

		require.Contains(t, byRef, "intro-interval")
		assert.Equal(t, TriggerKindInterval, byRef["intro-interval"].TriggerKind)
		assert.Equal(t, time.Hour, byRef["intro-interval"].Interval)

		require.Contains(t, byRef, "intro-cron")
		assert.Equal(t, TriggerKindCron, byRef["intro-cron"].TriggerKind)
		assert.Equal(t, "0 0 * * * *", byRef["intro-cron"].Expression)

		// the envelope only carries the target's name, so that is what Address reports
		// for a rebuilt schedule.
		assert.Equal(t, pid.Name(), byRef["intro-once"].Address)
	})

	t.Run("WithClusterSingleFire is carried in the envelope and fails open outside cluster mode", func(t *testing.T) {
		sys, pid := newPersistentTestSystem(t, "persist-singlefire", "target")

		queue := newFakeJobQueue(sys)
		locker := new(sync.Mutex)
		sched := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		sched.Start(context.TODO())

		message := new(testpb.TestSend)
		require.NoError(t, sched.ScheduleOnce(message, pid, 300*time.Millisecond,
			WithReference("singlefire-ref"), WithClusterSingleFire()))

		// the option must be persisted in the envelope, or the single-fire guarantee
		// would silently vanish for every schedule rebuilt after a restart.
		jobs, err := queue.ScheduledJobs(nil)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		msgJob, ok := jobs[0].JobDetail().Job().(*scheduledMessageJob)
		require.True(t, ok)
		assert.True(t, msgJob.envelope.GetClusterSingleFire())

		// outside cluster mode the claim fails open: delivery happens exactly as
		// without the option.
		pause.For(700 * time.Millisecond)
		assert.EqualValues(t, 1, pid.ProcessedCount()-1)
		sched.Stop(context.TODO())
	})

	t.Run("Schedule interval survives a scheduler Stop/Start cycle", func(t *testing.T) {
		sys, pid := newPersistentTestSystem(t, "persist-interval", "target")

		queue := newFakeJobQueue(sys)
		locker := new(sync.Mutex)
		sched := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		sched.Start(context.TODO())

		message := new(testpb.TestSend)
		require.NoError(t, sched.Schedule(message, pid, 300*time.Millisecond, WithReference("interval-ref")))

		sched.Stop(context.TODO())

		restarted := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		restarted.Start(context.TODO())
		defer restarted.Stop(context.TODO())

		_, ok := restarted.scheduledKeys.Get("interval-ref")
		assert.True(t, ok)

		pause.For(700 * time.Millisecond)
		assert.GreaterOrEqual(t, int(pid.ProcessedCount()-1), 1)
	})

	t.Run("ScheduleWithCron survives a scheduler Stop/Start cycle", func(t *testing.T) {
		sys, pid := newPersistentTestSystem(t, "persist-cron", "target")

		queue := newFakeJobQueue(sys)
		locker := new(sync.Mutex)
		sched := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		sched.Start(context.TODO())

		message := new(testpb.TestSend)
		const expr = "* * * ? * *" // fires every second
		require.NoError(t, sched.ScheduleWithCron(message, pid, expr, WithReference("cron-ref")))

		sched.Stop(context.TODO())

		restarted := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, locker)
		restarted.Start(context.TODO())
		defer restarted.Stop(context.TODO())

		_, ok := restarted.scheduledKeys.Get("cron-ref")
		assert.True(t, ok)

		pause.For(1500 * time.Millisecond)
		assert.GreaterOrEqual(t, int(pid.ProcessedCount()-1), 1)
	})

	t.Run("ScheduleOnce rejects a non proto.Message when a persistent queue is configured", func(t *testing.T) {
		sys, pid := newPersistentTestSystem(t, "persist-badmsg", "target")

		queue := newFakeJobQueue(sys)
		sched := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, new(sync.Mutex))
		sched.Start(context.TODO())
		defer sched.Stop(context.TODO())

		err := sched.ScheduleOnce("not-a-proto-message", pid, 100*time.Millisecond)
		assert.ErrorIs(t, err, errors.ErrScheduledMessageNotProto)
	})

	t.Run("Schedule and ScheduleWithCron also reject a non proto.Message", func(t *testing.T) {
		sys, pid := newPersistentTestSystem(t, "persist-badmsg-2", "target")

		queue := newFakeJobQueue(sys)
		sched := newScheduler(log.DiscardLogger, DefaultShutdownTimeout, sys, queue, new(sync.Mutex))
		sched.Start(context.TODO())
		defer sched.Stop(context.TODO())

		assert.ErrorIs(t, sched.Schedule(42, pid, 100*time.Millisecond), errors.ErrScheduledMessageNotProto)
		assert.ErrorIs(t, sched.ScheduleWithCron(42, pid, "* * * ? * *"), errors.ErrScheduledMessageNotProto)
	})

	t.Run("DecodeScheduledMessage rejects a malformed description", func(t *testing.T) {
		_, err := DecodeScheduledMessage("not-a-goakt-scheduled-message")
		assert.ErrorIs(t, err, errors.ErrInvalidScheduledMessage)
	})

	t.Run("DecodeScheduledMessage rejects corrupt base64 payload", func(t *testing.T) {
		_, err := DecodeScheduledMessage("GoAktScheduledMessage" + quartz.Sep + "not-valid-base64!!")
		assert.ErrorIs(t, err, errors.ErrInvalidScheduledMessage)
	})

	t.Run("scheduledMessageJob.Execute fails for an unresolvable target", func(t *testing.T) {
		sys, _ := newPersistentTestSystem(t, "persist-unresolvable", "target")

		payload, err := remote.NewProtoSerializer().Serialize(new(testpb.TestSend))
		require.NoError(t, err)

		envelope := &internalpb.ScheduledMessage{
			Reference:  "unresolvable-ref",
			TargetName: "does-not-exist",
			Payload:    payload,
		}

		jobInstance := NewScheduledMessageJob(sys, envelope)
		assert.Error(t, jobInstance.Execute(context.TODO()))
	})

	t.Run("scheduledMessageJob.Execute fails for a corrupt payload", func(t *testing.T) {
		sys, pid := newPersistentTestSystem(t, "persist-badpayload", "target")

		envelope := &internalpb.ScheduledMessage{
			Reference:  "bad-payload-ref",
			TargetName: pid.Name(),
			Payload:    []byte("not-a-valid-frame"),
		}

		jobInstance := NewScheduledMessageJob(sys, envelope)
		assert.Error(t, jobInstance.Execute(context.TODO()))
	})

	t.Run("ScheduledMessageTrigger reconstructs all three trigger kinds", func(t *testing.T) {
		once, err := ScheduledMessageTrigger(&internalpb.ScheduledMessage{
			Trigger: &internalpb.ScheduleTrigger{
				Kind: &internalpb.ScheduleTrigger_Once{Once: &internalpb.OnceTrigger{}},
			},
		})
		require.NoError(t, err)
		_, ok := once.(*quartz.RunOnceTrigger)
		assert.True(t, ok)

		interval, err := ScheduledMessageTrigger(&internalpb.ScheduledMessage{
			Trigger: &internalpb.ScheduleTrigger{
				Kind: &internalpb.ScheduleTrigger_Interval{Interval: &internalpb.IntervalTrigger{}},
			},
		})
		require.NoError(t, err)
		_, ok = interval.(*quartz.SimpleTrigger)
		assert.True(t, ok)

		cron, err := ScheduledMessageTrigger(&internalpb.ScheduledMessage{
			Trigger: &internalpb.ScheduleTrigger{
				Kind: &internalpb.ScheduleTrigger_Cron{Cron: &internalpb.CronTrigger{Expression: "* * * ? * *"}},
			},
		})
		require.NoError(t, err)
		_, ok = cron.(*quartz.CronTrigger)
		assert.True(t, ok)

		_, err = ScheduledMessageTrigger(&internalpb.ScheduledMessage{Trigger: &internalpb.ScheduleTrigger{}})
		assert.ErrorIs(t, err, errors.ErrInvalidScheduledMessage)
	})
}

// TestSchedulerPersistentJobQueueWithCluster proves WithSchedulerJobQueue
// composes with cluster mode: the envelope-based job still resolves its
// target (via the same ActorSystem.ActorOf used for cluster-wide lookups) and
// delivers the message. This mirrors the single-node, mocked-discovery
// pattern already used by the "when cluster is enabled" cases in
// scheduler_test.go; a true multi-node testkit.NewMultiNodes scenario is not
// applicable here because WithSchedulerJobQueue has no cluster-specific
// behavior of its own beyond that existing ActorOf resolution path, and
// testkit.StartNode does not currently accept custom ActorSystem options to
// inject a shared persistent queue across nodes.
func TestSchedulerPersistentJobQueueWithCluster(t *testing.T) {
	ctx := context.TODO()
	nodePorts := dynaport.Get(3)
	discoveryPort := nodePorts[0]
	clusterPort := nodePorts[1]
	remotingPort := nodePorts[2]

	logger := log.DiscardLogger
	host := "127.0.0.1"

	addrs := []string{net.JoinHostPort(host, strconv.Itoa(discoveryPort))}
	provider := new(testkit.Provider)

	queue := newFakeJobQueue(nil)
	locker := new(sync.Mutex)

	sys, err := NewActorSystem(
		"test-persistent-cluster",
		WithLogger(logger),
		WithRemote(remote.NewConfig(host, remotingPort)),
		WithCluster(
			NewClusterConfig().
				WithKinds(new(MockActor)).
				WithPartitionCount(9).
				WithReplicaCount(1).
				WithPeersPort(clusterPort).
				WithMinimumPeersQuorum(1).
				WithDiscoveryPort(discoveryPort).
				WithDiscovery(provider)),
		WithSchedulerJobQueue(queue, locker),
	)
	require.NoError(t, err)
	// the queue needs the started ActorSystem to resolve targets at fire time.
	queue.system = sys

	provider.EXPECT().ID().Return("testDisco")
	provider.EXPECT().Initialize().Return(nil)
	provider.EXPECT().Register().Return(nil)
	provider.EXPECT().Deregister().Return(nil)
	provider.EXPECT().DiscoverPeers().Return(addrs, nil)
	provider.EXPECT().Close().Return(nil)

	require.NoError(t, sys.Start(ctx))

	pause.For(time.Second)

	actorName := "target"
	actorRef, err := sys.Spawn(ctx, actorName, NewMockActor())
	require.NoError(t, err)

	pause.For(time.Second)

	message := new(testpb.TestSend)
	require.NoError(t, sys.ScheduleOnce(ctx, message, actorRef, 200*time.Millisecond, WithReference("cluster-once-ref")))

	pause.For(700 * time.Millisecond)
	assert.EqualValues(t, 1, actorRef.ProcessedCount()-1)

	require.NoError(t, sys.Stop(ctx))
	provider.AssertExpectations(t)
}

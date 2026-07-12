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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/tochemey/goakt/v4/internal/address"
	"github.com/tochemey/goakt/v4/internal/cluster"
	"github.com/tochemey/goakt/v4/internal/internalpb"
	"github.com/tochemey/goakt/v4/internal/types"
	"github.com/tochemey/goakt/v4/log"
	mockcluster "github.com/tochemey/goakt/v4/mocks/cluster"
	"github.com/tochemey/goakt/v4/placement"
)

// TestWithPlacementJournalOption verifies the option wires the journal onto
// the ActorSystem and that a nil journal is a no-op, leaving crash relocation
// off (today's default behavior).
func TestWithPlacementJournalOption(t *testing.T) {
	t.Run("With a configured journal", func(t *testing.T) {
		journal := placement.NewInMemoryJournal()
		system, err := NewActorSystem("test", WithLogger(log.DiscardLogger), WithPlacementJournal(journal))
		require.NoError(t, err)

		sys := system.(*actorSystem)
		require.Same(t, journal, sys.getPlacementJournal())
	})

	t.Run("With a nil journal", func(t *testing.T) {
		system, err := NewActorSystem("test", WithLogger(log.DiscardLogger), WithPlacementJournal(nil))
		require.NoError(t, err)

		sys := system.(*actorSystem)
		require.Nil(t, sys.getPlacementJournal())
	})

	t.Run("Without the option", func(t *testing.T) {
		system, err := NewActorSystem("test", WithLogger(log.DiscardLogger))
		require.NoError(t, err)

		sys := system.(*actorSystem)
		require.Nil(t, sys.getPlacementJournal())
	})
}

// TestRecordActorPlacement verifies putActorOnCluster journals a relocatable
// actor's placement when a journal is configured, and skips journaling in
// every case that must leave today's default behavior unchanged.
func TestRecordActorPlacement(t *testing.T) {
	ctx := context.Background()

	newRelocatablePID := func(sys *actorSystem, name string) *PID {
		pid := &PID{
			actorSystem: sys,
			actor:       NewMockActor(),
			address:     address.New(name, sys.Name(), "127.0.0.1", 8080),
		}
		// actors are relocatable by default; mirror that here since this test
		// builds a bare PID directly instead of going through newPID/Spawn.
		pid.setState(relocationState, true)
		return pid
	}

	t.Run("Records a relocatable actor when cluster and journal are enabled", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal

		pid := newRelocatablePID(sys, "victim")
		require.NoError(t, sys.putActorOnCluster(pid))

		entries, err := journal.ListByNode(ctx, sys.PeersAddress())
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, pid.ID(), entries[0].ID)
		assert.Equal(t, placement.KindActor, entries[0].Kind)

		wireActor := new(internalpb.Actor)
		require.NoError(t, proto.Unmarshal(entries[0].Payload, wireActor))
		assert.Equal(t, pid.ID(), wireActor.GetAddress())
		assert.True(t, wireActor.GetRelocatable())
	})

	t.Run("Skips journaling when no journal is configured", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		// sys.placementJournal left nil: default, unconfigured behavior

		pid := newRelocatablePID(sys, "victim")
		require.NoError(t, sys.putActorOnCluster(pid))
		// nothing to assert against a journal - absence of a panic/error is
		// the point: recording must be a safe no-op when unconfigured
	})

	t.Run("Skips journaling a non-relocatable actor", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal

		pid := newRelocatablePID(sys, "victim")
		pid.setState(relocationState, false)
		require.NoError(t, sys.putActorOnCluster(pid))

		entries, err := journal.ListByNode(ctx, sys.PeersAddress())
		require.NoError(t, err)
		require.Empty(t, entries)
	})

	t.Run("Skips journaling a system actor", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal

		pid := newRelocatablePID(sys, "victim")
		pid.setState(systemState, true)
		require.NoError(t, sys.putActorOnCluster(pid))

		entries, err := journal.ListByNode(ctx, sys.PeersAddress())
		require.NoError(t, err)
		require.Empty(t, entries)
	})

	t.Run("Skips journaling entirely when cluster is disabled", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		sys.clusterEnabled.Store(false)
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal

		pid := newRelocatablePID(sys, "victim")
		require.NoError(t, sys.putActorOnCluster(pid))

		entries, err := journal.ListByNode(ctx, sys.PeersAddress())
		require.NoError(t, err)
		require.Empty(t, entries)
	})
}

// erroringJournal is a placement.Journal whose Record always fails, used to
// prove recording is best-effort: a journal store outage must never fail an
// actor or grain spawn.
type erroringJournal struct {
	recordErr error
}

func (j *erroringJournal) Record(context.Context, *placement.Entry) error { return j.recordErr }
func (j *erroringJournal) Delete(context.Context, string, string) error   { return nil }
func (j *erroringJournal) ListByNode(context.Context, string) ([]*placement.Entry, error) {
	return nil, nil
}
func (j *erroringJournal) DeleteByNode(context.Context, string) error { return nil }

// TestRecordActorPlacement_JournalErrorDoesNotFailSpawn verifies that a
// placement journal store failure is logged and swallowed rather than
// propagated: putActorOnCluster/putGrainOnCluster must still succeed, since
// the journal is a best-effort crash-recovery aid, not a spawn precondition.
func TestRecordActorPlacement_JournalErrorDoesNotFailSpawn(t *testing.T) {
	t.Run("Actor variant", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		sys.placementJournal = &erroringJournal{recordErr: errors.New("journal store unavailable")}

		pid := &PID{
			actorSystem: sys,
			actor:       NewMockActor(),
			address:     address.New("victim", sys.Name(), "127.0.0.1", 8080),
		}
		pid.setState(relocationState, true)

		require.NoError(t, sys.putActorOnCluster(pid))
	})

	t.Run("Grain variant", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		sys.placementJournal = &erroringJournal{recordErr: errors.New("journal store unavailable")}

		identity := newGrainIdentity(new(MockGrain), "grain-err")
		gPID := &grainPID{
			identity:    identity,
			actorSystem: sys,
			config:      newGrainConfig(),
		}

		require.NoError(t, sys.putGrainOnCluster(gPID))
	})
}

// TestDeleteActorPlacement verifies deleteActorPlacement removes the
// recorded entry, and is a safe no-op without a configured journal.
func TestDeleteActorPlacement(t *testing.T) {
	ctx := context.Background()
	clusterMock := mockcluster.NewCluster(t)
	sys := MockReplicationTestSystem(clusterMock)
	journal := placement.NewInMemoryJournal()
	sys.placementJournal = journal

	require.NoError(t, journal.Record(ctx, &placement.Entry{
		ID:   "some-actor-id",
		Node: sys.PeersAddress(),
		Kind: placement.KindActor,
	}))

	sys.deleteActorPlacement(ctx, "some-actor-id")

	entries, err := journal.ListByNode(ctx, sys.PeersAddress())
	require.NoError(t, err)
	require.Empty(t, entries)

	// deleting again, or without a journal configured, must not panic
	sys.deleteActorPlacement(ctx, "some-actor-id")
	sys.placementJournal = nil
	sys.deleteActorPlacement(ctx, "some-actor-id")
}

// TestRecordGrainPlacement verifies putGrainOnCluster journals a grain's
// placement unless it opted out of relocation.
func TestRecordGrainPlacement(t *testing.T) {
	ctx := context.Background()

	t.Run("Records a grain when journal is configured", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal

		identity := newGrainIdentity(new(MockGrain), "grain-1")
		gPID := &grainPID{
			identity:    identity,
			actorSystem: sys,
			config:      newGrainConfig(),
		}

		require.NoError(t, sys.putGrainOnCluster(gPID))

		entries, err := journal.ListByNode(ctx, sys.PeersAddress())
		require.NoError(t, err)
		require.Len(t, entries, 1)
		assert.Equal(t, identity.String(), entries[0].ID)
		assert.Equal(t, placement.KindGrain, entries[0].Kind)
	})

	t.Run("Skips journaling a grain with relocation disabled", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal

		identity := newGrainIdentity(new(MockGrain), "grain-2")
		gPID := &grainPID{
			identity:    identity,
			actorSystem: sys,
			config:      newGrainConfig(WithGrainDisableRelocation()),
		}

		require.NoError(t, sys.putGrainOnCluster(gPID))

		entries, err := journal.ListByNode(ctx, sys.PeersAddress())
		require.NoError(t, err)
		require.Empty(t, entries)
	})
}

// TestResolveCrashedNodeState verifies the fallback that lets a leader
// replay a dead node's journal when the cluster store has no graceful
// state for it, and that it still prefers graceful state when available.
func TestResolveCrashedNodeState(t *testing.T) {
	ctx := context.Background()
	deadNode := "127.0.0.1:19999"

	buildEntry := func(t *testing.T, id string, kind placement.Kind) *placement.Entry {
		t.Helper()
		var payload []byte
		var err error
		switch kind {
		case placement.KindActor:
			payload, err = proto.Marshal(&internalpb.Actor{
				Address:     address.New(id, "test", "127.0.0.1", 9000).String(),
				Type:        types.Name(new(MockActor)),
				Relocatable: true,
			})
		case placement.KindGrain:
			payload, err = proto.Marshal(&internalpb.Grain{
				GrainId: &internalpb.GrainId{Value: id},
			})
		}
		require.NoError(t, err)
		return &placement.Entry{ID: id, Node: deadNode, Kind: kind, Payload: payload}
	}

	t.Run("Falls back to the journal when the cluster store has no state", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		sys.clusterStore = &recordingPeerStateStore{}
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal

		require.NoError(t, journal.Record(ctx, buildEntry(t, "actor-1", placement.KindActor)))
		require.NoError(t, journal.Record(ctx, buildEntry(t, "grain-1", placement.KindGrain)))

		peerState, ok := sys.resolveCrashedNodeState(ctx, deadNode)
		require.True(t, ok)
		require.NotNil(t, peerState)
		assert.Equal(t, "127.0.0.1", peerState.GetHost())
		assert.EqualValues(t, 19999, peerState.GetPeersPort())
		require.Len(t, peerState.GetActors(), 1)
		require.Len(t, peerState.GetGrains(), 1)
	})

	t.Run("Prefers graceful cluster store state over the journal", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		gracefulState := &internalpb.PeerState{Host: "127.0.0.1", PeersPort: 19999}
		sys.clusterStore = &fakePeerStateStore{state: gracefulState, found: true}
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal
		require.NoError(t, journal.Record(ctx, buildEntry(t, "actor-1", placement.KindActor)))

		peerState, ok := sys.resolveCrashedNodeState(ctx, deadNode)
		require.True(t, ok)
		assert.Same(t, gracefulState, peerState)
	})

	t.Run("Reports nothing found without a configured journal", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		sys.clusterStore = &recordingPeerStateStore{}
		// sys.placementJournal left nil: default, unconfigured behavior

		peerState, ok := sys.resolveCrashedNodeState(ctx, deadNode)
		require.False(t, ok)
		require.Nil(t, peerState)
	})

	t.Run("Reports nothing found when the journal has no entries for the node", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		sys.clusterStore = &recordingPeerStateStore{}
		sys.placementJournal = placement.NewInMemoryJournal()

		peerState, ok := sys.resolveCrashedNodeState(ctx, deadNode)
		require.False(t, ok)
		require.Nil(t, peerState)
	})
}

// TestBuildPeerStateFromJournal exercises decoding, including the case
// where a single corrupt entry must not prevent every other entry from
// being replayed.
func TestBuildPeerStateFromJournal(t *testing.T) {
	ctx := context.Background()
	deadNode := "127.0.0.1:19999"

	t.Run("Skips a corrupt payload but keeps the rest", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal

		goodPayload, err := proto.Marshal(&internalpb.Actor{
			Address:     address.New("good", "test", "127.0.0.1", 9000).String(),
			Relocatable: true,
		})
		require.NoError(t, err)

		require.NoError(t, journal.Record(ctx, &placement.Entry{ID: "good", Node: deadNode, Kind: placement.KindActor, Payload: goodPayload}))
		require.NoError(t, journal.Record(ctx, &placement.Entry{ID: "corrupt", Node: deadNode, Kind: placement.KindActor, Payload: []byte("not-a-proto-message")}))

		peerState, err := sys.buildPeerStateFromJournal(ctx, deadNode)
		require.NoError(t, err)
		require.NotNil(t, peerState)
		require.Len(t, peerState.GetActors(), 1)
		_, ok := peerState.GetActors()["good"]
		require.True(t, ok)
	})

	t.Run("Returns nil state when there are no entries", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		sys.placementJournal = placement.NewInMemoryJournal()

		peerState, err := sys.buildPeerStateFromJournal(ctx, deadNode)
		require.NoError(t, err)
		require.Nil(t, peerState)
	})

	t.Run("Errors on an invalid node address", func(t *testing.T) {
		clusterMock := mockcluster.NewCluster(t)
		sys := MockReplicationTestSystem(clusterMock)
		journal := placement.NewInMemoryJournal()
		sys.placementJournal = journal
		require.NoError(t, journal.Record(ctx, &placement.Entry{ID: "a", Node: "not-a-host-port", Kind: placement.KindActor, Payload: []byte("x")}))

		peerState, err := sys.buildPeerStateFromJournal(ctx, "not-a-host-port")
		require.Error(t, err)
		require.Nil(t, peerState)
	})
}

type fakePeerStateStore struct {
	state *internalpb.PeerState
	found bool
}

func (s *fakePeerStateStore) PersistPeerState(context.Context, *internalpb.PeerState) error {
	return nil
}
func (s *fakePeerStateStore) GetPeerState(context.Context, string) (*internalpb.PeerState, bool) {
	return s.state, s.found
}
func (s *fakePeerStateStore) DeletePeerState(context.Context, string) error { return nil }
func (s *fakePeerStateStore) Close() error                                  { return nil }

// TestCrashRelocationReplaysJournalForDeadNode is the fallback authorized by
// design: testkit cannot simulate a non-graceful node death (kill -9), so
// this test invokes the actual NodeLeft handler on a real, running,
// single-node cluster with synthesized journal state standing in for a node
// that crashed without ever running preShutdown. It proves the leader
// replays the journal through the same relocation machinery used for
// graceful departures, spawning the actor locally.
func TestCrashRelocationReplaysJournalForDeadNode(t *testing.T) {
	ctx := context.Background()
	srv := startNatsServer(t)

	journal := placement.NewInMemoryJournal()
	node, sd := testNATs(t, srv.Addr().String(), withTestPlacementJournal(journal))
	require.NotNil(t, node)
	require.NotNil(t, sd)

	sys := node.(*actorSystem)

	// this address never actually ran on this process: it stands in for a
	// node that crashed (kill -9) before ever pushing graceful PeerState.
	deadNodeAddr := "127.0.0.1:64999"
	actorName := "phoenix"

	wireActor := &internalpb.Actor{
		Address:     address.New(actorName, sys.Name(), "127.0.0.1", 20000).String(),
		Type:        types.Name(new(MockActor)),
		Relocatable: true,
	}
	payload, err := proto.Marshal(wireActor)
	require.NoError(t, err)
	require.NoError(t, journal.Record(ctx, &placement.Entry{
		ID:      wireActor.GetAddress(),
		Node:    deadNodeAddr,
		Kind:    placement.KindActor,
		Payload: payload,
	}))

	exists, err := node.ActorExists(ctx, actorName)
	require.NoError(t, err)
	require.False(t, exists, "the actor must not exist yet - it only lives in the synthesized journal")

	event := &cluster.Event{
		Type:    cluster.NodeLeft,
		Payload: &cluster.NodeLeftEvent{Address: deadNodeAddr, Timestamp: time.Now()},
	}
	sys.handleNodeLeftEvent(event)

	require.Eventually(t, func() bool {
		exists, err := node.ActorExists(ctx, actorName)
		return err == nil && exists
	}, 30*time.Second, 200*time.Millisecond, "actor %s should be respawned by replaying the placement journal of the dead node", actorName)

	// the dead node's journal entries are cleaned up once relocation completes
	require.Eventually(t, func() bool {
		entries, err := journal.ListByNode(ctx, deadNodeAddr)
		return err == nil && len(entries) == 0
	}, 30*time.Second, 200*time.Millisecond, "the dead node's journal entries should be cleared after relocation completes")

	assert.NoError(t, node.Stop(ctx))
	assert.NoError(t, sd.Close())
	srv.Shutdown()
}

// TestCrashRelocationReplayWithUnregisteredActorType proves that a single
// journaled actor whose type was never registered on the surviving node does
// not prevent every other journaled entry for the same dead node from being
// replayed: the bad entry is skipped, the good one is still respawned.
func TestCrashRelocationReplayWithUnregisteredActorType(t *testing.T) {
	ctx := context.Background()
	srv := startNatsServer(t)

	journal := placement.NewInMemoryJournal()
	node, sd := testNATs(t, srv.Addr().String(), withTestPlacementJournal(journal))
	require.NotNil(t, node)
	require.NotNil(t, sd)

	sys := node.(*actorSystem)

	// this address never actually ran on this process: it stands in for a
	// node that crashed (kill -9) before ever pushing graceful PeerState.
	deadNodeAddr := "127.0.0.1:64998"

	goodName := "phoenix"
	wireActor := &internalpb.Actor{
		Address:     address.New(goodName, sys.Name(), "127.0.0.1", 20000).String(),
		Type:        types.Name(new(MockActor)),
		Relocatable: true,
	}
	goodPayload, err := proto.Marshal(wireActor)
	require.NoError(t, err)
	require.NoError(t, journal.Record(ctx, &placement.Entry{
		ID:      wireActor.GetAddress(),
		Node:    deadNodeAddr,
		Kind:    placement.KindActor,
		Payload: goodPayload,
	}))

	ghostName := "ghost"
	ghostActor := &internalpb.Actor{
		Address:     address.New(ghostName, sys.Name(), "127.0.0.1", 20001).String(),
		Type:        "no.such.actortype.exists",
		Relocatable: true,
	}
	ghostPayload, err := proto.Marshal(ghostActor)
	require.NoError(t, err)
	require.NoError(t, journal.Record(ctx, &placement.Entry{
		ID:      ghostActor.GetAddress(),
		Node:    deadNodeAddr,
		Kind:    placement.KindActor,
		Payload: ghostPayload,
	}))

	event := &cluster.Event{
		Type:    cluster.NodeLeft,
		Payload: &cluster.NodeLeftEvent{Address: deadNodeAddr, Timestamp: time.Now()},
	}
	sys.handleNodeLeftEvent(event)

	require.Eventually(t, func() bool {
		exists, err := node.ActorExists(ctx, goodName)
		return err == nil && exists
	}, 30*time.Second, 200*time.Millisecond, "actor %s should be respawned despite the unregistered-type entry sharing its dead node", goodName)

	exists, err := node.ActorExists(ctx, ghostName)
	require.NoError(t, err)
	require.False(t, exists, "the entry referencing an unregistered actor type must never be spawned")

	assert.NoError(t, node.Stop(ctx))
	assert.NoError(t, sd.Close())
	srv.Shutdown()
}

// Without a configured journal, resolveCrashedNodeState finds nothing for a
// crashed node (no graceful cluster-store state either); the "no journal"
// subtest of TestResolveCrashedNodeState covers that directly. The
// NodeLeft handler then falls through to the registry-derived crash recovery
// (deriveRelocationSetFromRegistry, exercised directly in
// actor_system_test.go), so there is no "nothing happens" default left to
// assert here now that crash recovery is on by default.

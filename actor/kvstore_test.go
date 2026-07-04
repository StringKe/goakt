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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gerrors "github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/internal/cluster"
	"github.com/tochemey/goakt/v4/kv"
	"github.com/tochemey/goakt/v4/log"
	mockscluster "github.com/tochemey/goakt/v4/mocks/cluster"
)

// fakeLock is a minimal cluster.Lock implementation used to exercise TryLock/Unlock
// translation without depending on the real cluster engine.
type fakeLock struct {
	unlockErr error
}

func (l *fakeLock) Unlock(context.Context) error { return l.unlockErr }

func TestKV(t *testing.T) {
	t.Run("When actor system is not started", func(t *testing.T) {
		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)

		store, err := sys.KV()
		require.Error(t, err)
		assert.ErrorIs(t, err, gerrors.ErrActorSystemNotStarted)
		assert.Nil(t, store)
	})
	t.Run("When cluster is disabled", func(t *testing.T) {
		ctx := context.Background()
		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)

		require.NoError(t, sys.Start(ctx))
		defer func() { require.NoError(t, sys.Stop(ctx)) }()

		store, err := sys.KV()
		require.Error(t, err)
		assert.ErrorIs(t, err, gerrors.ErrClusterDisabled)
		assert.Nil(t, store)
	})
	t.Run("Get translates ErrKVKeyNotFound", func(t *testing.T) {
		ctx := context.Background()
		clusterMock := mockscluster.NewCluster(t)
		clusterMock.EXPECT().GetKV(ctx, "missing").Return(nil, cluster.ErrKVKeyNotFound)

		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))

		sysImpl := sys.(*actorSystem)
		sysImpl.cluster = clusterMock
		sysImpl.clusterEnabled.Store(true)

		store, err := sys.KV()
		require.NoError(t, err)
		require.NotNil(t, store)

		value, err := store.Get(ctx, "missing")
		require.Nil(t, value)
		assert.ErrorIs(t, err, kv.ErrKeyNotFound)
	})
	t.Run("Get returns the stored value", func(t *testing.T) {
		ctx := context.Background()
		clusterMock := mockscluster.NewCluster(t)
		clusterMock.EXPECT().GetKV(ctx, "widget").Return([]byte("node-1"), nil)

		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))

		sysImpl := sys.(*actorSystem)
		sysImpl.cluster = clusterMock
		sysImpl.clusterEnabled.Store(true)

		store, err := sys.KV()
		require.NoError(t, err)

		value, err := store.Get(ctx, "widget")
		require.NoError(t, err)
		assert.Equal(t, []byte("node-1"), value)
	})
	t.Run("Put forwards the resolved TTL", func(t *testing.T) {
		ctx := context.Background()
		clusterMock := mockscluster.NewCluster(t)
		clusterMock.EXPECT().PutKV(ctx, "widget", []byte("node-1"), 30*time.Second).Return(nil)

		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))

		sysImpl := sys.(*actorSystem)
		sysImpl.cluster = clusterMock
		sysImpl.clusterEnabled.Store(true)

		store, err := sys.KV()
		require.NoError(t, err)
		require.NoError(t, store.Put(ctx, "widget", []byte("node-1"), kv.WithTTL(30*time.Second)))
	})
	t.Run("PutIfAbsent translates ErrKVKeyExists", func(t *testing.T) {
		ctx := context.Background()
		clusterMock := mockscluster.NewCluster(t)
		clusterMock.EXPECT().PutKVIfAbsent(ctx, "widget", []byte("node-1"), time.Duration(0)).Return(cluster.ErrKVKeyExists)

		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))

		sysImpl := sys.(*actorSystem)
		sysImpl.cluster = clusterMock
		sysImpl.clusterEnabled.Store(true)

		store, err := sys.KV()
		require.NoError(t, err)

		err = store.PutIfAbsent(ctx, "widget", []byte("node-1"))
		assert.ErrorIs(t, err, kv.ErrKeyExists)
	})
	t.Run("Delete forwards to the cluster engine", func(t *testing.T) {
		ctx := context.Background()
		clusterMock := mockscluster.NewCluster(t)
		clusterMock.EXPECT().DeleteKV(ctx, "widget").Return(nil)

		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))

		sysImpl := sys.(*actorSystem)
		sysImpl.cluster = clusterMock
		sysImpl.clusterEnabled.Store(true)

		store, err := sys.KV()
		require.NoError(t, err)
		require.NoError(t, store.Delete(ctx, "widget"))
	})
	t.Run("TryLock translates ErrLockNotAcquired", func(t *testing.T) {
		ctx := context.Background()
		clusterMock := mockscluster.NewCluster(t)
		clusterMock.EXPECT().TryLock(ctx, "cert:example.com", 5*time.Second).Return(nil, cluster.ErrLockNotAcquired)

		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))

		sysImpl := sys.(*actorSystem)
		sysImpl.cluster = clusterMock
		sysImpl.clusterEnabled.Store(true)

		store, err := sys.KV()
		require.NoError(t, err)

		lock, err := store.TryLock(ctx, "cert:example.com", 5*time.Second)
		require.Nil(t, lock)
		assert.ErrorIs(t, err, kv.ErrLockNotAcquired)
	})
	t.Run("TryLock succeeds and Unlock translates ErrLockNotHeld", func(t *testing.T) {
		ctx := context.Background()
		underlying := &fakeLock{unlockErr: cluster.ErrLockNotHeld}
		clusterMock := mockscluster.NewCluster(t)
		clusterMock.EXPECT().TryLock(ctx, "cert:example.com", 5*time.Second).Return(underlying, nil)

		sys, err := NewActorSystem("testSys", WithLogger(log.DiscardLogger))
		require.NoError(t, err)
		require.NoError(t, sys.Start(ctx))

		sysImpl := sys.(*actorSystem)
		sysImpl.cluster = clusterMock
		sysImpl.clusterEnabled.Store(true)

		store, err := sys.KV()
		require.NoError(t, err)

		lock, err := store.TryLock(ctx, "cert:example.com", 5*time.Second)
		require.NoError(t, err)
		require.NotNil(t, lock)

		err = lock.Unlock(ctx)
		assert.ErrorIs(t, err, kv.ErrLockNotHeld)
	})
}

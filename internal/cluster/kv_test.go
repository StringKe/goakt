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

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/discovery"
	dynaport "github.com/tochemey/goakt/v4/internal/net"
	"github.com/tochemey/goakt/v4/internal/pause"
	"github.com/tochemey/goakt/v4/log"
	mocksdiscovery "github.com/tochemey/goakt/v4/mocks/discovery"
)

// newSingleNodeKVCluster starts a single-node cluster engine for KV/lock tests and
// registers cleanup to stop it.
func newSingleNodeKVCluster(t *testing.T) Cluster {
	t.Helper()

	ctx := context.Background()
	ports := dynaport.Get(3)
	gossipPort := ports[0]
	clusterPort := ports[1]
	remotingPort := ports[2]

	addrs := []string{fmt.Sprintf("127.0.0.1:%d", gossipPort)}

	provider := new(mocksdiscovery.Provider)
	provider.EXPECT().ID().Return("testDisco")
	provider.EXPECT().Initialize().Return(nil)
	provider.EXPECT().Register().Return(nil)
	provider.EXPECT().Deregister().Return(nil)
	provider.EXPECT().DiscoverPeers().Return(addrs, nil)
	provider.EXPECT().Close().Return(nil)

	host := "127.0.0.1"
	node := &discovery.Node{
		Name:          host,
		Host:          host,
		DiscoveryPort: gossipPort,
		PeersPort:     clusterPort,
		RemotingPort:  remotingPort,
	}

	cl := New("test", provider, node, WithLogger(log.DiscardLogger))
	require.NoError(t, cl.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, cl.Stop(stopCtx))
		provider.AssertExpectations(t)
	})

	return cl
}

func TestPutKVGetKVDeleteKV(t *testing.T) {
	ctx := context.Background()
	cl := newSingleNodeKVCluster(t)

	// unknown key
	_, err := cl.GetKV(ctx, "missing")
	require.ErrorIs(t, err, ErrKVKeyNotFound)

	// put and get
	require.NoError(t, cl.PutKV(ctx, "registry:widget", []byte("node-1"), 0))
	value, err := cl.GetKV(ctx, "registry:widget")
	require.NoError(t, err)
	assert.Equal(t, []byte("node-1"), value)

	// overwrite
	require.NoError(t, cl.PutKV(ctx, "registry:widget", []byte("node-2"), 0))
	value, err = cl.GetKV(ctx, "registry:widget")
	require.NoError(t, err)
	assert.Equal(t, []byte("node-2"), value)

	// delete
	require.NoError(t, cl.DeleteKV(ctx, "registry:widget"))
	_, err = cl.GetKV(ctx, "registry:widget")
	require.ErrorIs(t, err, ErrKVKeyNotFound)

	// deleting a missing key is a no-op
	require.NoError(t, cl.DeleteKV(ctx, "registry:widget"))
}

func TestPutKVIfAbsent(t *testing.T) {
	ctx := context.Background()
	cl := newSingleNodeKVCluster(t)

	require.NoError(t, cl.PutKVIfAbsent(ctx, "dedup:msg-1", []byte("seen"), 0))

	value, err := cl.GetKV(ctx, "dedup:msg-1")
	require.NoError(t, err)
	assert.Equal(t, []byte("seen"), value)

	// second writer loses the race
	err = cl.PutKVIfAbsent(ctx, "dedup:msg-1", []byte("seen-again"), 0)
	require.ErrorIs(t, err, ErrKVKeyExists)

	// the original value is untouched
	value, err = cl.GetKV(ctx, "dedup:msg-1")
	require.NoError(t, err)
	assert.Equal(t, []byte("seen"), value)
}

func TestPutKVTTLExpiry(t *testing.T) {
	ctx := context.Background()
	cl := newSingleNodeKVCluster(t)

	require.NoError(t, cl.PutKV(ctx, "session:1", []byte("data"), 300*time.Millisecond))

	value, err := cl.GetKV(ctx, "session:1")
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), value)

	pause.For(700 * time.Millisecond)

	_, err = cl.GetKV(ctx, "session:1")
	require.ErrorIs(t, err, ErrKVKeyNotFound)
}

func TestTryLock(t *testing.T) {
	ctx := context.Background()
	cl := newSingleNodeKVCluster(t)

	lock, err := cl.TryLock(ctx, "cert:example.com", 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, lock)

	// a second holder cannot acquire the same key while it is held
	_, err = cl.TryLock(ctx, "cert:example.com", 5*time.Second)
	require.ErrorIs(t, err, ErrLockNotAcquired)

	// releasing it frees the key up for a new holder
	require.NoError(t, lock.Unlock(ctx))

	lock2, err := cl.TryLock(ctx, "cert:example.com", 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, lock2.Unlock(ctx))

	// unlocking an already-released lock reports ErrLockNotHeld
	err = lock2.Unlock(ctx)
	require.ErrorIs(t, err, ErrLockNotHeld)
}

// TestPutKVGetKVSpecialCharacterKeys pins the behavior of keys that are not
// plain identifiers: colons (already used elsewhere as a namespacing
// convention within a single key), unicode, and the empty string.
func TestPutKVGetKVSpecialCharacterKeys(t *testing.T) {
	ctx := context.Background()
	cl := newSingleNodeKVCluster(t)

	t.Run("Colons inside the key round-trip", func(t *testing.T) {
		key := "tenant:acme:widget:7"
		require.NoError(t, cl.PutKV(ctx, key, []byte("v1"), 0))
		value, err := cl.GetKV(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte("v1"), value)
	})

	t.Run("Unicode key round-trips", func(t *testing.T) {
		key := "用户:名前:🚀"
		require.NoError(t, cl.PutKV(ctx, key, []byte("unicode-value"), 0))
		value, err := cl.GetKV(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte("unicode-value"), value)
	})

	t.Run("Empty string key round-trips like any other key", func(t *testing.T) {
		key := ""
		require.NoError(t, cl.PutKV(ctx, key, []byte("empty-key-value"), 0))
		value, err := cl.GetKV(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, []byte("empty-key-value"), value)

		require.NoError(t, cl.DeleteKV(ctx, key))
		_, err = cl.GetKV(ctx, key)
		require.ErrorIs(t, err, ErrKVKeyNotFound)
	})
}

// TestPutIsNotBlockedByTryLock pins the advisory-lock contract: TryLock
// guards a caller-defined convention (composeKey(namespaceLocks, key)), a
// namespace entirely separate from the KV data itself
// (composeKey(namespaceKV, key)). Holding a lock for a given logical key must
// never block a Put for that same logical key - the lock is a cooperative
// signal between callers, not an enforcement mechanism over the KV store.
func TestPutIsNotBlockedByTryLock(t *testing.T) {
	ctx := context.Background()
	cl := newSingleNodeKVCluster(t)

	key := "cert:example.com"
	lock, err := cl.TryLock(ctx, key, 5*time.Second)
	require.NoError(t, err)
	require.NotNil(t, lock)

	done := make(chan error, 1)
	go func() {
		done <- cl.PutKV(ctx, key, []byte("issued"), 0)
	}()

	select {
	case err := <-done:
		require.NoError(t, err, "Put on a locked key must succeed instead of blocking on the lock")
	case <-time.After(2 * time.Second):
		t.Fatal("Put on a locked key blocked instead of succeeding immediately")
	}

	value, err := cl.GetKV(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("issued"), value)

	require.NoError(t, lock.Unlock(ctx))
}

func TestTryLockExpiry(t *testing.T) {
	ctx := context.Background()
	cl := newSingleNodeKVCluster(t)

	lock, err := cl.TryLock(ctx, "job:nightly", 300*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, lock)

	pause.For(700 * time.Millisecond)

	// the lock auto-expired, so another holder can now acquire it
	lock2, err := cl.TryLock(ctx, "job:nightly", 5*time.Second)
	require.NoError(t, err)
	require.NoError(t, lock2.Unlock(ctx))

	// unlocking the original, already-expired lock reports ErrLockNotHeld
	err = lock.Unlock(ctx)
	require.ErrorIs(t, err, ErrLockNotHeld)
}

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

package benchmark

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/discovery/selfmanaged"
	dynaport "github.com/tochemey/goakt/v4/internal/net"
	"github.com/tochemey/goakt/v4/internal/pause"
	"github.com/tochemey/goakt/v4/kv"
	"github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/remote"
)

// newBenchKVCluster starts a single-node, self-managed-discovery cluster and returns
// its kv.Store. b.Cleanup stops the actor system when the benchmark finishes.
func newBenchKVCluster(b *testing.B) kv.Store {
	b.Helper()

	ctx := context.Background()
	ports := dynaport.Get(3)
	discoveryPort := ports[0]
	remotingPort := ports[1]
	peersPort := ports[2]
	host := "127.0.0.1"

	provider := selfmanaged.NewDiscovery(&selfmanaged.Config{
		ClusterName:       "kv-bench",
		SelfAddress:       fmt.Sprintf("%s:%d", host, discoveryPort),
		BroadcastPort:     ports[0],
		BroadcastInterval: 100 * time.Millisecond,
		BroadcastAddress:  net.IPv4(127, 0, 0, 1),
	})

	clusterConfig := actor.NewClusterConfig().
		WithKinds(new(noopActor)).
		WithPartitionCount(7).
		WithReplicaCount(1).
		WithPeersPort(peersPort).
		WithMinimumPeersQuorum(1).
		WithDiscoveryPort(discoveryPort).
		WithClusterStateSyncInterval(300 * time.Millisecond).
		WithDiscovery(provider)

	system, err := actor.NewActorSystem("kv-bench",
		actor.WithLogger(log.DiscardLogger),
		actor.WithCluster(clusterConfig),
		actor.WithRemote(remote.NewConfig(host, remotingPort)))
	require.NoError(b, err)
	require.NoError(b, system.Start(ctx))
	pause.For(2 * time.Second)

	b.Cleanup(func() {
		_ = system.Stop(ctx)
	})

	store, err := system.KV()
	require.NoError(b, err)
	return store
}

func BenchmarkKVPut(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping cluster benchmark in short mode")
	}

	ctx := context.Background()
	store := newBenchKVCluster(b)
	value := []byte("node-1")

	b.ResetTimer()
	for i := range b.N {
		key := "bench:put:" + strconv.Itoa(i)
		if err := store.Put(ctx, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVGet(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping cluster benchmark in short mode")
	}

	ctx := context.Background()
	store := newBenchKVCluster(b)
	require.NoError(b, store.Put(ctx, "bench:get:key", []byte("node-1")))

	b.ResetTimer()
	for range b.N {
		if _, err := store.Get(ctx, "bench:get:key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVLockAcquireRelease(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping cluster benchmark in short mode")
	}

	ctx := context.Background()
	store := newBenchKVCluster(b)

	b.ResetTimer()
	for i := range b.N {
		key := "bench:lock:" + strconv.Itoa(i)
		lock, err := store.TryLock(ctx, key, 30*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if err := lock.Unlock(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

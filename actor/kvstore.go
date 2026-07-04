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
	"time"

	gerrors "github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/internal/cluster"
	"github.com/tochemey/goakt/v4/kv"
)

// kvStore adapts the actor system's embedded cluster engine to the public kv.Store
// interface, translating internal/cluster sentinel errors into their kv package
// equivalents.
type kvStore struct {
	system *actorSystem
}

// enforce compilation error
var _ kv.Store = (*kvStore)(nil)

// checkAvailable reports whether the actor system is running with cluster mode
// enabled, returning the appropriate typed error otherwise.
func (s *kvStore) checkAvailable() error {
	if !s.system.Running() {
		return gerrors.ErrActorSystemNotStarted
	}
	if !s.system.clusterEnabled.Load() {
		return gerrors.ErrClusterDisabled
	}
	return nil
}

// Get retrieves the value stored under key from the cluster key/value registry.
func (s *kvStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.checkAvailable(); err != nil {
		return nil, err
	}

	value, err := s.system.cluster.GetKV(ctx, key)
	if err != nil {
		if errors.Is(err, cluster.ErrKVKeyNotFound) {
			return nil, kv.ErrKeyNotFound
		}
		return nil, err
	}
	return value, nil
}

// Put stores value under key in the cluster key/value registry.
func (s *kvStore) Put(ctx context.Context, key string, value []byte, opts ...kv.PutOption) error {
	if err := s.checkAvailable(); err != nil {
		return err
	}

	ttl := kv.ResolveTTL(opts...)
	return s.system.cluster.PutKV(ctx, key, value, ttl)
}

// PutIfAbsent stores value under key only if the key is not already present.
func (s *kvStore) PutIfAbsent(ctx context.Context, key string, value []byte, opts ...kv.PutOption) error {
	if err := s.checkAvailable(); err != nil {
		return err
	}

	ttl := kv.ResolveTTL(opts...)
	if err := s.system.cluster.PutKVIfAbsent(ctx, key, value, ttl); err != nil {
		if errors.Is(err, cluster.ErrKVKeyExists) {
			return kv.ErrKeyExists
		}
		return err
	}
	return nil
}

// Delete removes the value stored under key from the cluster key/value registry.
func (s *kvStore) Delete(ctx context.Context, key string) error {
	if err := s.checkAvailable(); err != nil {
		return err
	}

	return s.system.cluster.DeleteKV(ctx, key)
}

// TryLock attempts to acquire a distributed lock for key, held for at most ttl.
func (s *kvStore) TryLock(ctx context.Context, key string, ttl time.Duration) (kv.Lock, error) {
	if err := s.checkAvailable(); err != nil {
		return nil, err
	}

	lock, err := s.system.cluster.TryLock(ctx, key, ttl)
	if err != nil {
		if errors.Is(err, cluster.ErrLockNotAcquired) {
			return nil, kv.ErrLockNotAcquired
		}
		return nil, err
	}
	return &kvLock{lock: lock}, nil
}

// kvLock adapts an internal/cluster.Lock to the public kv.Lock interface.
type kvLock struct {
	lock cluster.Lock
}

// enforce compilation error
var _ kv.Lock = (*kvLock)(nil)

// Unlock releases the lock, translating cluster.ErrLockNotHeld into kv.ErrLockNotHeld.
func (l *kvLock) Unlock(ctx context.Context) error {
	if err := l.lock.Unlock(ctx); err != nil {
		if errors.Is(err, cluster.ErrLockNotHeld) {
			return kv.ErrLockNotHeld
		}
		return err
	}
	return nil
}

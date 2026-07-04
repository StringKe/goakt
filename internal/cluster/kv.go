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
	"errors"
	"time"

	"github.com/tochemey/olric"
)

// PutKV stores a raw value in the cluster-scoped key/value registry.
func (x *cluster) PutKV(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if !x.running.Load() {
		return ErrEngineNotRunning
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	return x.putRecordTTL(ctx, namespaceKV, key, value, ttl)
}

// PutKVIfAbsent stores a raw value in the cluster-scoped key/value registry only if the key
// is not already present.
func (x *cluster) PutKVIfAbsent(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if !x.running.Load() {
		return ErrEngineNotRunning
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	if err := x.putRecordIfAbsentTTL(ctx, namespaceKV, key, value, ttl); err != nil {
		if errors.Is(err, olric.ErrKeyFound) {
			return ErrKVKeyExists
		}
		return err
	}
	return nil
}

// GetKV retrieves the raw value stored under the given key in the cluster-scoped key/value
// registry.
func (x *cluster) GetKV(ctx context.Context, key string) ([]byte, error) {
	if !x.running.Load() {
		return nil, ErrEngineNotRunning
	}

	x.mu.RLock()
	defer x.mu.RUnlock()

	value, err := x.getRecord(ctx, namespaceKV, key)
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return nil, ErrKVKeyNotFound
		}
		return nil, err
	}
	return value, nil
}

// DeleteKV removes the value stored under the given key in the cluster-scoped key/value
// registry.
func (x *cluster) DeleteKV(ctx context.Context, key string) error {
	if !x.running.Load() {
		return ErrEngineNotRunning
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	return x.deleteRecord(ctx, namespaceKV, key)
}

// TryLock attempts to acquire a distributed lock for the given key. The attempt is
// non-blocking: it makes a single try to set the lock and returns ErrLockNotAcquired
// immediately if another holder already owns it, rather than waiting for it to free up.
// The lock auto-expires after ttl if Unlock is never called, so a crashed holder cannot
// wedge the key forever.
func (x *cluster) TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error) {
	if !x.running.Load() {
		return nil, ErrEngineNotRunning
	}

	x.mu.RLock()
	dm := x.dmap
	x.mu.RUnlock()

	lockCtx := context.WithoutCancel(ctx)
	lockCtx, cancel := context.WithTimeout(lockCtx, x.writeTimeout)
	defer cancel()

	// deadline=0 makes the acquisition attempt non-blocking: olric tries once and,
	// if the key is already locked, fails immediately instead of polling until deadline.
	lockContext, err := dm.LockWithTimeout(lockCtx, composeKey(namespaceLocks, key), ttl, 0)
	if err != nil {
		if errors.Is(err, olric.ErrLockNotAcquired) {
			return nil, ErrLockNotAcquired
		}
		return nil, err
	}

	return &olricLock{lockContext: lockContext}, nil
}

// olricLock adapts an olric.LockContext to the Lock interface, translating engine-specific
// errors into GoAkt sentinel errors.
type olricLock struct {
	lockContext olric.LockContext
}

// Unlock releases the lock, translating olric.ErrNoSuchLock into ErrLockNotHeld.
func (l *olricLock) Unlock(ctx context.Context) error {
	if err := l.lockContext.Unlock(ctx); err != nil {
		if errors.Is(err, olric.ErrNoSuchLock) {
			return ErrLockNotHeld
		}
		return err
	}
	return nil
}

// putRecordTTL writes a namespaced record to the unified map applying timeouts and an
// optional TTL. A zero or negative ttl means the entry never expires.
func (x *cluster) putRecordTTL(ctx context.Context, namespace recordNamespace, key string, value []byte, ttl time.Duration) error {
	ctx = context.WithoutCancel(ctx)
	ctx, cancel := context.WithTimeout(ctx, x.writeTimeout)
	defer cancel()

	return x.dmap.Put(ctx, composeKey(namespace, key), value, putTTLOptions(ttl)...)
}

// putRecordIfAbsentTTL writes a namespaced record to the unified map only if the key is
// absent, applying an optional TTL.
func (x *cluster) putRecordIfAbsentTTL(ctx context.Context, namespace recordNamespace, key string, value []byte, ttl time.Duration) error {
	ctx = context.WithoutCancel(ctx)
	ctx, cancel := context.WithTimeout(ctx, x.writeTimeout)
	defer cancel()

	opts := append(putTTLOptions(ttl), olric.NX())
	return x.dmap.Put(ctx, composeKey(namespace, key), value, opts...)
}

// putTTLOptions builds the olric put options for the given TTL.
func putTTLOptions(ttl time.Duration) []olric.PutOption {
	if ttl > 0 {
		return []olric.PutOption{olric.EX(ttl)}
	}
	return nil
}

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

// Package kv exposes a narrow, cluster-scoped key/value registry and distributed
// lock backed by the actor system's embedded cluster engine.
//
// Store is positioned as a registry for application metadata -- ownership records,
// idempotency/dedup keys, short-lived coordination locks -- and not as a general
// purpose cache. Reads are strongly consistent with respect to the underlying
// engine's quorum settings, unlike the eventually-consistent crdt package.
package kv

import (
	"context"
	"time"
)

// Store is a cluster-scoped key/value registry and distributed lock, obtained via
// actor.ActorSystem.KV. All methods require cluster mode to be enabled on the actor
// system that produced the Store.
type Store interface {
	// Get retrieves the value stored under key. It returns ErrKeyNotFound if the key
	// does not exist or has expired.
	Get(ctx context.Context, key string) ([]byte, error)

	// Put stores value under key, overwriting any existing value. WithTTL may be passed
	// to make the entry expire automatically after the given duration; without it the
	// entry never expires.
	Put(ctx context.Context, key string, value []byte, opts ...PutOption) error

	// PutIfAbsent stores value under key only if the key is not already present, and
	// reports ErrKeyExists otherwise. This is the primitive for cluster-wide uniqueness
	// and dedup/idempotency checks: combined with WithTTL it doubles as a seen-keys
	// store where entries self-expire.
	PutIfAbsent(ctx context.Context, key string, value []byte, opts ...PutOption) error

	// Delete removes the value stored under key. It is a no-op if the key does not exist.
	Delete(ctx context.Context, key string) error

	// TryLock attempts to acquire a distributed lock for key, held for at most ttl.
	// The attempt is non-blocking: if the key is already locked by another holder,
	// TryLock returns ErrLockNotAcquired immediately rather than waiting for it to
	// free up. The lock automatically expires after ttl even if Unlock is never
	// called, so a crashed holder cannot wedge the key forever.
	TryLock(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}

// Lock represents a distributed lock acquired through Store.TryLock.
type Lock interface {
	// Unlock releases the lock, allowing other holders to acquire it. It returns
	// ErrLockNotHeld if the lock already expired or was already released.
	Unlock(ctx context.Context) error
}

// putConfig holds the options applied to Put and PutIfAbsent.
type putConfig struct {
	ttl time.Duration
}

// PutOption configures optional behavior for Store.Put and Store.PutIfAbsent.
type PutOption func(*putConfig)

// WithTTL makes the stored entry expire automatically after the given duration.
func WithTTL(ttl time.Duration) PutOption {
	return func(c *putConfig) {
		c.ttl = ttl
	}
}

// ResolveTTL folds the given PutOption values into the TTL they configure. It is
// exported so that Store implementations outside this package (e.g. the actor
// package's cluster-backed Store) can honor WithTTL without depending on putConfig's
// internal layout.
func ResolveTTL(opts ...PutOption) time.Duration {
	var cfg putConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg.ttl
}

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

package kv

import "errors"

var (
	// ErrKeyNotFound is returned by Get when the key does not exist or has expired.
	ErrKeyNotFound = errors.New("kv: key not found")

	// ErrKeyExists is returned by PutIfAbsent when the key is already present.
	ErrKeyExists = errors.New("kv: key already exists")

	// ErrLockNotAcquired is returned by TryLock when the lock is already held by
	// another holder.
	ErrLockNotAcquired = errors.New("kv: lock not acquired")

	// ErrLockNotHeld is returned by Lock.Unlock when the lock already expired or was
	// already released.
	ErrLockNotHeld = errors.New("kv: lock not held")
)

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

import "errors"

var (
	// ErrJobNotFound is returned when a Store operation references a job id that
	// does not exist.
	ErrJobNotFound = errors.New("jobs: job not found")

	// ErrJobAlreadyExists is returned by Enqueue/EnqueueFanOut when a job with the
	// same id has already been recorded.
	ErrJobAlreadyExists = errors.New("jobs: job already exists")

	// ErrInvalidMaxAttempts is returned when a RetryPolicy has a non-positive
	// MaxAttempts; there is no "unlimited retries" sentinel.
	ErrInvalidMaxAttempts = errors.New("jobs: max attempts must be at least 1")

	// ErrInvalidQueue is returned by NewJob when the queue name is empty.
	ErrInvalidQueue = errors.New("jobs: queue name must not be empty")

	// ErrInvalidTargetName is returned by NewJob when the target actor/grain name
	// is empty.
	ErrInvalidTargetName = errors.New("jobs: target name must not be empty")

	// ErrPayloadNotProto is returned when a job's message does not implement
	// proto.Message, or when a leased job's payload cannot be decoded back into
	// one.
	ErrPayloadNotProto = errors.New("jobs: payload must implement proto.Message")

	// ErrEmptyChildren is returned by EnqueueFanOut when called with no children,
	// which would otherwise create a parent that can never leave StateWaiting.
	ErrEmptyChildren = errors.New("jobs: fan-out requires at least one child job")

	// ErrStoreRequired is returned by NewEngine when no Store is configured.
	ErrStoreRequired = errors.New("jobs: a Store is required")

	// ErrActorSystemRequired is returned by NewEngine when no ActorSystem is
	// supplied.
	ErrActorSystemRequired = errors.New("jobs: an ActorSystem is required")

	// ErrEngineAlreadyStarted is returned by Engine.Start when the engine is
	// already running.
	ErrEngineAlreadyStarted = errors.New("jobs: engine already started")

	// ErrNotRetryable is returned by Inspector.Retry when the job is not in a
	// state Retry can act on (only StateDeadLetter and StatePending jobs can be
	// force-retried).
	ErrNotRetryable = errors.New("jobs: job is not in a retryable state")
)

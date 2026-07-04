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

package jobs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/goakt/v4/jobs"
	"github.com/tochemey/goakt/v4/test/data/testpb"
)

func TestJobEnvelopeRoundTrip(t *testing.T) {
	original, err := jobs.NewJob("rewards", "aggregator", &testpb.TestSum{A: 1, B: 2},
		jobs.WithRetryPolicy(jobs.LinearBackoff(time.Second, time.Minute, 4)))
	require.NoError(t, err)
	original.Attempts = 2
	original.State = jobs.StateLeased
	original.LeaseOwner = "worker-1"
	original.LastError = "boom"
	original.ParentID = "parent-1"
	original.ChildIndex = 3

	envelope := original.ToEnvelope()
	restored := jobs.FromEnvelope(envelope)

	require.Equal(t, original.ID, restored.ID)
	require.Equal(t, original.Queue, restored.Queue)
	require.Equal(t, original.TargetName, restored.TargetName)
	require.Equal(t, original.Payload, restored.Payload)
	require.Equal(t, original.RetryPolicy, restored.RetryPolicy)
	require.Equal(t, original.Attempts, restored.Attempts)
	require.Equal(t, original.State, restored.State)
	require.Equal(t, original.LeaseOwner, restored.LeaseOwner)
	require.Equal(t, original.LastError, restored.LastError)
	require.Equal(t, original.ParentID, restored.ParentID)
	require.Equal(t, original.ChildIndex, restored.ChildIndex)
	require.WithinDuration(t, original.CreatedAt, restored.CreatedAt, time.Millisecond)
	require.WithinDuration(t, original.AvailableAt, restored.AvailableAt, time.Millisecond)
}

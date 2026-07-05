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

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// memoryStore is the built-in, process-local Store implementation. It keeps
// every job in a map guarded by a single mutex: correct and simple, at the
// cost of O(n) Lease/List/QueueDepths scans over all known jobs. A durable
// adapter backed by a real database should index by (queue, state,
// available_at) instead.
//
// Jobs are never actually removed from memory except via Delete: terminal
// jobs (StateSucceeded, StateDeadLetter) remain visible to Get/List/Inspector
// until explicitly deleted, so operators can inspect recent history.
type memoryStore struct {
	mu sync.Mutex

	jobs      map[string]*Job
	children  map[string][]string // parentID -> child job ids, in order
	remaining map[string]int      // parentID -> outstanding (non-terminal) child count
	results   map[string][]ChildResult

	// leaseTokenSeq is the source of the fencing token Lease hands out on every
	// successful grant/reclaim; it only ever increases, so a stale token can
	// never collide with the job's current one.
	leaseTokenSeq int64
}

// NewMemoryStore returns the built-in in-memory Store implementation. It is
// the default Store an Engine uses when none is configured, and is
// appropriate for single-process deployments or tests; state does not survive
// a process restart.
func NewMemoryStore() Store {
	return &memoryStore{
		jobs:      make(map[string]*Job),
		children:  make(map[string][]string),
		remaining: make(map[string]int),
		results:   make(map[string][]ChildResult),
	}
}

func (s *memoryStore) Enqueue(_ context.Context, job *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.ID]; exists {
		return ErrJobAlreadyExists
	}
	s.jobs[job.ID] = job.Clone()
	return nil
}

func (s *memoryStore) EnqueueFanOut(_ context.Context, parent *Job, children []*Job) error {
	if len(children) == 0 {
		return ErrEmptyChildren
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[parent.ID]; exists {
		return ErrJobAlreadyExists
	}
	for _, child := range children {
		if _, exists := s.jobs[child.ID]; exists {
			return ErrJobAlreadyExists
		}
	}

	parentClone := parent.Clone()
	parentClone.State = StateWaiting
	parentClone.ChildCount = len(children)

	childIDs := make([]string, len(children))
	for i, child := range children {
		childClone := child.Clone()
		childClone.ParentID = parentClone.ID
		childClone.ChildIndex = i
		s.jobs[childClone.ID] = childClone
		childIDs[i] = childClone.ID
	}

	s.jobs[parentClone.ID] = parentClone
	s.children[parentClone.ID] = childIDs
	s.remaining[parentClone.ID] = len(children)
	s.results[parentClone.ID] = make([]ChildResult, len(children))
	return nil
}

func (s *memoryStore) Lease(_ context.Context, queue, owner string, leaseTTL time.Duration, limit int) ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	leased := make([]*Job, 0)
	for _, job := range s.jobs {
		if limit > 0 && len(leased) >= limit {
			break
		}
		if queue != "" && job.Queue != queue {
			continue
		}

		switch job.State {
		case StatePending:
			if job.AvailableAt.After(now) {
				continue
			}
		case StateLeased:
			if job.LeaseExpiresAt.After(now) {
				continue
			}
			// lease expired: reclaim it for redelivery.
		default:
			continue
		}

		s.leaseTokenSeq++
		job.State = StateLeased
		job.LeaseOwner = owner
		job.LeaseExpiresAt = now.Add(leaseTTL)
		job.LeaseToken = s.leaseTokenSeq
		job.UpdatedAt = now
		leased = append(leased, job.Clone())
	}
	return leased, nil
}

func (s *memoryStore) Ack(_ context.Context, id string, token int64, result []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if err := checkLeaseLocked(job, token); err != nil {
		return err
	}

	job.State = StateSucceeded
	job.UpdatedAt = time.Now()

	if job.ParentID != "" {
		s.completeChildLocked(job.ParentID, job.ChildIndex, result, false, "")
	}
	return nil
}

func (s *memoryStore) Nack(_ context.Context, id string, token int64, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if err := checkLeaseLocked(job, token); err != nil {
		return err
	}

	job.Attempts++
	if cause != nil {
		job.LastError = cause.Error()
	}
	now := time.Now()
	job.UpdatedAt = now

	if job.Attempts >= job.RetryPolicy.MaxAttempts {
		job.State = StateDeadLetter
		if job.ParentID != "" {
			s.completeChildLocked(job.ParentID, job.ChildIndex, nil, true, job.LastError)
		}
		return nil
	}

	job.State = StatePending
	job.AvailableAt = now.Add(job.RetryPolicy.NextDelay(job.Attempts))
	return nil
}

func (s *memoryStore) DeadLetter(_ context.Context, id string, token int64, cause error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if err := checkLeaseLocked(job, token); err != nil {
		return err
	}

	job.State = StateDeadLetter
	if cause != nil {
		job.LastError = cause.Error()
	}
	job.UpdatedAt = time.Now()

	if job.ParentID != "" {
		s.completeChildLocked(job.ParentID, job.ChildIndex, nil, true, job.LastError)
	}
	return nil
}

// checkLeaseLocked enforces the fencing invariant shared by Ack/Nack/
// DeadLetter (Store invariants 4-6): a job already in a terminal state can
// never be re-finalized regardless of token (this is what stops a late Ack
// from resurrecting a dead-lettered job), and a non-terminal job can only be
// finalized by whoever holds its current LeaseToken -- a stale worker whose
// lease already expired and was reclaimed presents an old token that no
// longer matches. Must be called with the store's mutex held.
func checkLeaseLocked(job *Job, token int64) error {
	if job.State == StateSucceeded || job.State == StateDeadLetter {
		return ErrStaleLease
	}
	if job.LeaseToken != token {
		return ErrStaleLease
	}
	return nil
}

// completeChildLocked must be called with s.mu held. See Store's invariant 6
// for the semantics it implements.
func (s *memoryStore) completeChildLocked(parentID string, childIndex int, payload []byte, failed bool, errMsg string) {
	parent, ok := s.jobs[parentID]
	if !ok || parent.State != StateWaiting {
		return
	}

	results := s.results[parentID]
	if childIndex < 0 || childIndex >= len(results) {
		return
	}
	results[childIndex] = ChildResult{ChildIndex: childIndex, Payload: payload, Failed: failed, Error: errMsg}
	s.remaining[parentID]--

	if failed {
		parent.State = StateDeadLetter
		parent.LastError = fmt.Sprintf("fan-out child %d failed: %s", childIndex, errMsg)
		parent.UpdatedAt = time.Now()
		delete(s.remaining, parentID)
		delete(s.results, parentID)
		return
	}

	if s.remaining[parentID] > 0 {
		return
	}

	now := time.Now()
	parent.ChildResults = append([]ChildResult(nil), results...)
	parent.State = StatePending
	parent.AvailableAt = now
	parent.UpdatedAt = now
	delete(s.remaining, parentID)
}

func (s *memoryStore) Get(_ context.Context, id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}
	return job.Clone(), nil
}

func (s *memoryStore) List(_ context.Context, filter Filter) ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	matches := make([]*Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		if filter.Queue != "" && job.Queue != filter.Queue {
			continue
		}
		if filter.ParentID != "" && job.ParentID != filter.ParentID {
			continue
		}
		if len(filter.States) > 0 && !containsState(filter.States, job.State) {
			continue
		}
		matches = append(matches, job.Clone())
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i].CreatedAt.Before(matches[j].CreatedAt) })

	if filter.Offset > 0 {
		if filter.Offset >= len(matches) {
			return []*Job{}, nil
		}
		matches = matches[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(matches) {
		matches = matches[:filter.Limit]
	}
	return matches, nil
}

func (s *memoryStore) QueueDepths(_ context.Context) (map[string]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	depths := make(map[string]int)
	for _, job := range s.jobs {
		if job.State == StateSucceeded || job.State == StateDeadLetter {
			continue
		}
		depths[job.Queue]++
	}
	return depths, nil
}

func (s *memoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[id]; !ok {
		return ErrJobNotFound
	}
	delete(s.jobs, id)
	delete(s.children, id)
	delete(s.remaining, id)
	delete(s.results, id)
	return nil
}

func (s *memoryStore) Requeue(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}

	job.Attempts = 0
	job.LastError = ""
	job.State = StatePending
	job.AvailableAt = time.Now()
	job.UpdatedAt = job.AvailableAt
	return nil
}

func containsState(states []State, state State) bool {
	for _, s := range states {
		if s == state {
			return true
		}
	}
	return false
}

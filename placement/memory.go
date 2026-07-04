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

package placement

import (
	"context"
	"sync"
	"time"
)

// InMemoryJournal is a process-local Journal implementation. It is the
// default store used by tests and single-process clusters; it does not
// survive a process crash, so it cannot by itself provide crash relocation
// across node processes. Use it as a reference for custom durable adapters
// or for testkit-style tests where every simulated node shares the same
// process.
type InMemoryJournal struct {
	mu      sync.RWMutex
	entries map[string]map[string]*Entry // node -> id -> entry
}

// enforce compilation error
var _ Journal = (*InMemoryJournal)(nil)

// NewInMemoryJournal creates an empty InMemoryJournal.
func NewInMemoryJournal() *InMemoryJournal {
	return &InMemoryJournal{
		entries: make(map[string]map[string]*Entry),
	}
}

// Record implements Journal.
func (j *InMemoryJournal) Record(_ context.Context, entry *Entry) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	byID, ok := j.entries[entry.Node]
	if !ok {
		byID = make(map[string]*Entry)
		j.entries[entry.Node] = byID
	}

	clone := *entry
	if entry.RecordedAt.IsZero() {
		clone.RecordedAt = time.Now().UTC()
	}
	payload := make([]byte, len(entry.Payload))
	copy(payload, entry.Payload)
	clone.Payload = payload

	byID[entry.ID] = &clone
	return nil
}

// Delete implements Journal.
func (j *InMemoryJournal) Delete(_ context.Context, node, id string) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	byID, ok := j.entries[node]
	if !ok {
		return nil
	}
	delete(byID, id)
	if len(byID) == 0 {
		delete(j.entries, node)
	}
	return nil
}

// ListByNode implements Journal.
func (j *InMemoryJournal) ListByNode(_ context.Context, node string) ([]*Entry, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()

	byID := j.entries[node]
	out := make([]*Entry, 0, len(byID))
	for _, entry := range byID {
		clone := *entry
		payload := make([]byte, len(entry.Payload))
		copy(payload, entry.Payload)
		clone.Payload = payload
		out = append(out, &clone)
	}
	return out, nil
}

// DeleteByNode implements Journal.
func (j *InMemoryJournal) DeleteByNode(_ context.Context, node string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.entries, node)
	return nil
}

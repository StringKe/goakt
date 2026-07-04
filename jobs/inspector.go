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

import "context"

// Inspector is a read/admin surface over a Store meant for operator tooling:
// dashboards, CLIs, health checks. It has no privileged access -- it is a thin
// wrapper that only calls exported Store methods -- so any Store
// implementation gets an Inspector for free.
type Inspector struct {
	store Store
}

// NewInspector returns an Inspector backed by store.
func NewInspector(store Store) *Inspector {
	return &Inspector{store: store}
}

// List returns every job matching filter. See Filter for the available
// narrowing options (queue, states, parent job, pagination).
func (i *Inspector) List(ctx context.Context, filter Filter) ([]*Job, error) {
	return i.store.List(ctx, filter)
}

// Get returns a snapshot of a single job by id.
func (i *Inspector) Get(ctx context.Context, id string) (*Job, error) {
	return i.store.Get(ctx, id)
}

// Retry forces id back to StatePending with its attempt count reset, so it
// becomes leasable again on the next poll. Typically used to manually recover
// a StateDeadLetter job once the underlying issue has been fixed.
func (i *Inspector) Retry(ctx context.Context, id string) error {
	return i.store.Requeue(ctx, id)
}

// Delete permanently removes a job, regardless of its state.
func (i *Inspector) Delete(ctx context.Context, id string) error {
	return i.store.Delete(ctx, id)
}

// QueueDepths returns the number of non-terminal jobs per queue.
func (i *Inspector) QueueDepths(ctx context.Context) (map[string]int, error) {
	return i.store.QueueDepths(ctx)
}

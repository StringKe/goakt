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

// Package placement defines the placement journal used to relocate actors and
// grains after a cluster node dies without a graceful shutdown.
//
// GoAkt's built-in relocation (see the actor package) only runs when a node
// leaves cleanly: preShutdown snapshots the node's relocatable actors and
// grains and replicates that snapshot to peers before the node leaves
// membership. A crash (kill -9, OOM, hardware loss) never runs preShutdown,
// so nothing is replicated and the departed node's actors and grains are
// simply gone.
//
// A Journal closes that gap: the actor system records a placement Entry every
// time a relocatable actor is spawned or a grain is activated, and removes
// the entry once the actor or grain stops deliberately. When the cluster
// leader observes a NodeLeft for a node it has no graceful state for, it
// replays that node's journal entries through the same relocation machinery
// used for graceful departures.
package placement

import (
	"context"
	"time"
)

// Kind distinguishes the kind of entity a placement Entry describes.
type Kind uint8

const (
	// KindActor identifies a placement entry describing a plain relocatable actor.
	KindActor Kind = iota
	// KindGrain identifies a placement entry describing a grain.
	KindGrain
)

// String implements fmt.Stringer.
func (k Kind) String() string {
	switch k {
	case KindActor:
		return "actor"
	case KindGrain:
		return "grain"
	default:
		return "unknown"
	}
}

// Entry is a single recorded placement: it states that the actor or grain
// identified by ID is hosted on Node as of RecordedAt. Payload carries the
// wire-format snapshot needed to recreate the placement elsewhere (an
// encoded internal actor or grain record); a Journal implementation treats
// it as an opaque blob and must return it unmodified from ListByNode.
type Entry struct {
	// ID uniquely identifies the placement within Node: the actor ID for
	// KindActor entries, the grain identity string for KindGrain entries.
	ID string
	// Node is the cluster peer address (host:peersPort) hosting the
	// placement described by this entry.
	Node string
	// Kind distinguishes actor entries from grain entries.
	Kind Kind
	// Payload is the opaque, serialized placement snapshot.
	Payload []byte
	// RecordedAt is when the entry was written; informational only, it does
	// not affect replay.
	RecordedAt time.Time
}

// Journal records where relocatable actors and grains are placed so a
// surviving cluster leader can respawn them elsewhere after a node dies
// without a graceful shutdown.
//
// GoAkt ships NewInMemoryJournal, a process-local implementation suitable for
// tests and single-process clusters. Production deployments that need
// crash relocation across separate node processes must supply a Journal
// backed by storage every node can reach (a database, Redis, etc.) via
// ActorSystem's WithPlacementJournal option; GoAkt keeps that adapter outside
// core, following the same pattern as discovery.Provider.
//
// Implementations must be safe for concurrent use.
type Journal interface {
	// Record persists the given placement entry, creating or overwriting any
	// existing entry with the same Node and ID.
	Record(ctx context.Context, entry *Entry) error
	// Delete removes the placement entry identified by (node, id), if any.
	// Deleting an entry that does not exist is not an error.
	Delete(ctx context.Context, node, id string) error
	// ListByNode returns every placement entry currently recorded for node,
	// in no particular order. It returns an empty slice, not an error, when
	// node has no recorded entries.
	ListByNode(ctx context.Context, node string) ([]*Entry, error)
	// DeleteByNode removes every placement entry recorded for node. Callers
	// invoke it once a departed node's entries have been handed off for
	// replay (whether or not the replay succeeded) so the journal does not
	// keep growing across repeated crashes.
	DeleteByNode(ctx context.Context, node string) error
}

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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInMemoryJournalRecordAndListByNode(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()

	require.NoError(t, journal.Record(ctx, &Entry{
		ID:      "actor-1",
		Node:    "127.0.0.1:9000",
		Kind:    KindActor,
		Payload: []byte("actor-1-payload"),
	}))
	require.NoError(t, journal.Record(ctx, &Entry{
		ID:      "grain-1",
		Node:    "127.0.0.1:9000",
		Kind:    KindGrain,
		Payload: []byte("grain-1-payload"),
	}))
	require.NoError(t, journal.Record(ctx, &Entry{
		ID:      "actor-2",
		Node:    "127.0.0.1:9001",
		Kind:    KindActor,
		Payload: []byte("actor-2-payload"),
	}))

	entries, err := journal.ListByNode(ctx, "127.0.0.1:9000")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	byID := make(map[string]*Entry, len(entries))
	for _, entry := range entries {
		byID[entry.ID] = entry
	}
	require.Contains(t, byID, "actor-1")
	require.Contains(t, byID, "grain-1")
	require.Equal(t, KindActor, byID["actor-1"].Kind)
	require.Equal(t, []byte("actor-1-payload"), byID["actor-1"].Payload)
	require.Equal(t, KindGrain, byID["grain-1"].Kind)
	require.False(t, byID["actor-1"].RecordedAt.IsZero())

	// a different node's entries are not visible from this node's listing
	other, err := journal.ListByNode(ctx, "127.0.0.1:9001")
	require.NoError(t, err)
	require.Len(t, other, 1)
	require.Equal(t, "actor-2", other[0].ID)
}

func TestInMemoryJournalListByNodeUnknownNodeReturnsEmpty(t *testing.T) {
	journal := NewInMemoryJournal()
	entries, err := journal.ListByNode(context.Background(), "127.0.0.1:9999")
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestInMemoryJournalRecordOverwritesExistingEntry(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()

	require.NoError(t, journal.Record(ctx, &Entry{ID: "actor-1", Node: "n1", Payload: []byte("v1")}))
	require.NoError(t, journal.Record(ctx, &Entry{ID: "actor-1", Node: "n1", Payload: []byte("v2")}))

	entries, err := journal.ListByNode(ctx, "n1")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, []byte("v2"), entries[0].Payload)
}

func TestInMemoryJournalDelete(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()

	require.NoError(t, journal.Record(ctx, &Entry{ID: "actor-1", Node: "n1", Payload: []byte("v1")}))
	require.NoError(t, journal.Record(ctx, &Entry{ID: "actor-2", Node: "n1", Payload: []byte("v2")}))

	require.NoError(t, journal.Delete(ctx, "n1", "actor-1"))

	entries, err := journal.ListByNode(ctx, "n1")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "actor-2", entries[0].ID)

	// deleting an entry that does not exist is not an error
	require.NoError(t, journal.Delete(ctx, "n1", "does-not-exist"))
	require.NoError(t, journal.Delete(ctx, "unknown-node", "actor-1"))
}

func TestInMemoryJournalDeleteByNode(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()

	require.NoError(t, journal.Record(ctx, &Entry{ID: "actor-1", Node: "n1", Payload: []byte("v1")}))
	require.NoError(t, journal.Record(ctx, &Entry{ID: "actor-2", Node: "n1", Payload: []byte("v2")}))
	require.NoError(t, journal.Record(ctx, &Entry{ID: "actor-3", Node: "n2", Payload: []byte("v3")}))

	require.NoError(t, journal.DeleteByNode(ctx, "n1"))

	entries, err := journal.ListByNode(ctx, "n1")
	require.NoError(t, err)
	require.Empty(t, entries)

	entries, err = journal.ListByNode(ctx, "n2")
	require.NoError(t, err)
	require.Len(t, entries, 1)

	// deleting an unknown node is not an error
	require.NoError(t, journal.DeleteByNode(ctx, "unknown-node"))
}

func TestInMemoryJournalRecordClonesPayload(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()

	payload := []byte("original")
	require.NoError(t, journal.Record(ctx, &Entry{ID: "actor-1", Node: "n1", Payload: payload}))

	// mutating the caller's slice after Record must not affect the stored entry
	payload[0] = 'X'

	entries, err := journal.ListByNode(ctx, "n1")
	require.NoError(t, err)
	require.Equal(t, "original", string(entries[0].Payload))

	// mutating the returned entry's payload must not affect the store either
	entries[0].Payload[0] = 'Y'
	entries2, err := journal.ListByNode(ctx, "n1")
	require.NoError(t, err)
	require.Equal(t, "original", string(entries2[0].Payload))
}

func TestInMemoryJournalConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	journal := NewInMemoryJournal()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "actor"
			require.NoError(t, journal.Record(ctx, &Entry{ID: id, Node: "n1", Payload: []byte{byte(i)}}))
			_, err := journal.ListByNode(ctx, "n1")
			require.NoError(t, err)
			require.NoError(t, journal.Delete(ctx, "n1", id))
		}(i)
	}
	wg.Wait()
}

func TestKindString(t *testing.T) {
	require.Equal(t, "actor", KindActor.String())
	require.Equal(t, "grain", KindGrain.String())
	require.Equal(t, "unknown", Kind(99).String())
}

package compaction

import (
	"sort"

	"github.com/venkatanish04/tasksync/internal/lsm/memtable"
)

// Merge combines entries from multiple SSTables
// and keeps only the newest version of each key.
func Merge(entries []memtable.Entry) []memtable.Entry {
	if len(entries) == 0 {
		return nil
	}

	// Sort by key ascending.
	// For the same key, newest sequence comes first.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key == entries[j].Key {
			return entries[i].Sequence > entries[j].Sequence
		}

		return entries[i].Key < entries[j].Key
	})

	result := make([]memtable.Entry, 0)

	var previousKey string

	for _, entry := range entries {
		if entry.Key == previousKey {
			continue
		}

		result = append(result, entry)

		previousKey = entry.Key
	}

	return result
}

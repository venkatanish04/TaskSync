package memtable

import (
	"errors"
	"sort"
	"sync"
)

var ErrKeyNotFound = errors.New("key not found")

type Entry struct {
	Key       string
	Value     string
	Tombstone bool
	Sequence  uint64
}

type MemTable struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

func New() *MemTable {
	return &MemTable{
		entries: make(map[string]Entry),
	}
}

func (m *MemTable) Put(key string, value string, sequence uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries[key] = Entry{
		Key:       key,
		Value:     value,
		Tombstone: false,
		Sequence:  sequence,
	}
}

func (m *MemTable) Delete(key string, sequence uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries[key] = Entry{
		Key:       key,
		Value:     "",
		Tombstone: true,
		Sequence:  sequence,
	}
}

func (m *MemTable) Get(key string) (Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, exists := m.entries[key]

	if !exists {
		return Entry{}, ErrKeyNotFound
	}

	return entry, nil
}

func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.entries)
}

func (m *MemTable) Entries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Entry, 0, len(m.entries))

	for _, entry := range m.entries {
		result = append(result, entry)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = make(map[string]Entry)
}

package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/venkatanish04/tasksync/internal/lsm/compaction"
	"github.com/venkatanish04/tasksync/internal/lsm/memtable"
	"github.com/venkatanish04/tasksync/internal/lsm/sstable"
	"github.com/venkatanish04/tasksync/internal/lsm/wal"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

const (
	DefaultMemTableLimit = 5
)

// Storage represents the complete LSM-tree storage engine.
//
// Write path:
//
//	PUT/DELETE
//	    ↓
//	WAL
//	    ↓
//	MemTable
//	    ↓
//	SSTable
//
// Read path:
//
//	MemTable
//	    ↓
//	SSTables
//
// Older SSTables are searched after newer SSTables.
type Storage struct {
	mu sync.RWMutex

	dir string

	wal *wal.WAL

	memTable *memtable.MemTable

	sstables []*sstable.SSTable

	nextSequence uint64

	memTableLimit int
}

// Open creates or opens an existing LSM storage directory.
//
// If a WAL already exists, its operations are replayed into
// the MemTable to recover data after a crash.
func Open(dir string, memTableLimit int) (*Storage, error) {
	if memTableLimit <= 0 {
		memTableLimit = DefaultMemTableLimit
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "wal.log")

	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	s := &Storage{
		dir:           dir,
		wal:           w,
		memTable:      memtable.New(),
		sstables:      make([]*sstable.SSTable, 0),
		nextSequence:  0,
		memTableLimit: memTableLimit,
	}

	// Load existing SSTables.
	if err := s.loadSSTables(); err != nil {
		w.Close()
		return nil, fmt.Errorf("load SSTables: %w", err)
	}

	// Recover WAL operations.
	if err := s.recover(); err != nil {
		w.Close()
		return nil, fmt.Errorf("recover WAL: %w", err)
	}

	return s, nil
}

// Put stores or updates a key.
//
// The operation is first written to the WAL and then
// applied to the MemTable.
func (s *Storage) Put(key string, value string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextSequence++

	operation := wal.Operation{
		Type:  "PUT",
		Key:   key,
		Value: value,
	}

	if err := s.wal.Append(operation); err != nil {
		s.nextSequence--
		return fmt.Errorf("append PUT to WAL: %w", err)
	}

	s.memTable.Put(key, value, s.nextSequence)

	return s.flushIfNeeded()
}

// Delete removes a key logically by creating a tombstone.
//
// The tombstone is necessary because an older SSTable may still
// contain the previous value.
func (s *Storage) Delete(key string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextSequence++

	operation := wal.Operation{
		Type:  "DELETE",
		Key:   key,
		Value: "",
	}

	if err := s.wal.Append(operation); err != nil {
		s.nextSequence--
		return fmt.Errorf("append DELETE to WAL: %w", err)
	}

	s.memTable.Delete(key, s.nextSequence)

	return s.flushIfNeeded()
}

// Get retrieves the newest value for a key.
//
// Search order:
//
// 1. MemTable
// 2. Newest SSTable
// 3. Older SSTables
func (s *Storage) Get(key string) (string, error) {
	if key == "" {
		return "", errors.New("key cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Search MemTable first.
	entry, err := s.memTable.Get(key)

	if err == nil {
		if entry.Tombstone {
			return "", ErrKeyNotFound
		}

		return entry.Value, nil
	}

	if !errors.Is(err, memtable.ErrKeyNotFound) {
		return "", err
	}

	// Search SSTables from newest to oldest.
	for i := len(s.sstables) - 1; i >= 0; i-- {
		entry, err := s.sstables[i].Get(key)

		if err != nil {
			if errors.Is(err, sstable.ErrKeyNotFound) {
				continue
			}

			return "", err
		}

		if entry.Tombstone {
			return "", ErrKeyNotFound
		}

		return entry.Value, nil
	}

	return "", ErrKeyNotFound
}

// Flush writes the current MemTable to an SSTable.
//
// After successful SSTable creation, the MemTable is cleared.
func (s *Storage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.flush()
}

// flush performs the actual flush.
//
// The caller must hold s.mu.
func (s *Storage) flush() error {
	entries := s.memTable.Entries()

	if len(entries) == 0 {
		return nil
	}

	sstPath := filepath.Join(
		s.dir,
		fmt.Sprintf("sstable-%020d.sst", s.nextSequence),
	)

	_, err := sstable.Write(sstPath, entries)
	if err != nil {
		return fmt.Errorf("write SSTable: %w", err)
	}

	table := sstable.Open(sstPath)

	// New SSTable is always the newest.
	s.sstables = append(s.sstables, table)

	s.memTable.Clear()

	return nil
}

// flushIfNeeded automatically flushes when the MemTable
// reaches the configured size limit.
//
// The caller must hold s.mu.
func (s *Storage) flushIfNeeded() error {
	if s.memTable.Size() < s.memTableLimit {
		return nil
	}

	return s.flush()
}

// Compact merges all SSTables into one SSTable.
//
// For duplicate keys, the newest sequence number wins.
func (s *Storage) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.sstables) <= 1 {
		return nil
	}

	allEntries := make([]memtable.Entry, 0)

	for _, table := range s.sstables {
		entries, err := readAllEntries(table.Path())

		if err != nil {
			return fmt.Errorf("read SSTable during compaction: %w", err)
		}

		allEntries = append(allEntries, entries...)
	}

	merged := compaction.Merge(allEntries)

	compactedPath := filepath.Join(
		s.dir,
		fmt.Sprintf("compacted-%020d.sst", s.nextSequence),
	)

	_, err := sstable.Write(compactedPath, merged)
	if err != nil {
		return fmt.Errorf("write compacted SSTable: %w", err)
	}

	newTable := sstable.Open(compactedPath)

	// Remove old SSTables.
	for _, table := range s.sstables {
		if err := os.Remove(table.Path()); err != nil {
			return fmt.Errorf(
				"remove old SSTable %s: %w",
				table.Path(),
				err,
			)
		}
	}

	s.sstables = []*sstable.SSTable{
		newTable,
	}

	return nil
}

// Close safely closes the storage engine.
//
// Before closing:
//
// 1. Flush MemTable
// 2. Close WAL
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.flush(); err != nil {
		return err
	}

	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}

	return nil
}

// SSTableCount returns the current number of SSTables.
func (s *Storage) SSTableCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.sstables)
}

// MemTableSize returns the number of entries currently in
// the MemTable.
func (s *Storage) MemTableSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.memTable.Size()
}

// Sequence returns the latest sequence number.
func (s *Storage) Sequence() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.nextSequence
}

// loadSSTables discovers existing SSTable files.
func (s *Storage) loadSSTables() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	type tableInfo struct {
		path string
		seq  uint64
	}

	tables := make([]tableInfo, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		if !strings.HasSuffix(name, ".sst") {
			continue
		}

		seq, err := extractSequence(name)
		if err != nil {
			continue
		}

		tables = append(tables, tableInfo{
			path: filepath.Join(s.dir, name),
			seq:  seq,
		})

		if seq > s.nextSequence {
			s.nextSequence = seq
		}
	}

	sort.Slice(
		tables,
		func(i, j int) bool {
			return tables[i].seq < tables[j].seq
		},
	)

	for _, table := range tables {
		s.sstables = append(
			s.sstables,
			sstable.Open(table.path),
		)
	}

	return nil
}

// recover replays WAL operations into the MemTable.
func (s *Storage) recover() error {
	operations, err := s.wal.Replay()
	if err != nil {
		return err
	}

	for _, operation := range operations {
		s.nextSequence++

		switch operation.Type {
		case "PUT":
			s.memTable.Put(
				operation.Key,
				operation.Value,
				s.nextSequence,
			)

		case "DELETE":
			s.memTable.Delete(
				operation.Key,
				s.nextSequence,
			)

		default:
			return fmt.Errorf(
				"unknown WAL operation: %s",
				operation.Type,
			)
		}
	}

	return nil
}

// readAllEntries reads every record from an SSTable.
//
// This is used by compaction.
func readAllEntries(path string) ([]memtable.Entry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	if len(lines) == 1 && lines[0] == "" {
		return nil, nil
	}

	entries := make([]memtable.Entry, 0, len(lines))

	for _, line := range lines {
		parts := strings.SplitN(line, "|", 4)

		if len(parts) != 4 {
			return nil, fmt.Errorf(
				"invalid SSTable record: %q",
				line,
			)
		}

		tombstone := parts[2] == "true"

		var sequence uint64

		if _, err := fmt.Sscanf(
			parts[3],
			"%d",
			&sequence,
		); err != nil {
			return nil, fmt.Errorf(
				"invalid sequence number: %q",
				parts[3],
			)
		}

		entries = append(entries, memtable.Entry{
			Key:       parts[0],
			Value:     parts[1],
			Tombstone: tombstone,
			Sequence:  sequence,
		})
	}

	return entries, nil
}

// extractSequence extracts the numeric sequence from
// filenames such as:
//
//	sstable-00000000000000000005.sst
//	compacted-00000000000000000010.sst
func extractSequence(name string) (uint64, error) {
	name = strings.TrimSuffix(name, ".sst")

	index := strings.LastIndex(name, "-")

	if index == -1 {
		return 0, errors.New("invalid SSTable filename")
	}

	var sequence uint64

	if _, err := fmt.Sscanf(
		name[index+1:],
		"%d",
		&sequence,
	); err != nil {
		return 0, err
	}

	return sequence, nil
}

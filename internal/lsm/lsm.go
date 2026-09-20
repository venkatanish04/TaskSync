package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

// LSM represents the complete Log-Structured Merge Tree.
//
// Write path:
//
//	Client
//	   ↓
//	WAL
//	   ↓
//	MemTable
//	   ↓
//	SSTable
//	   ↓
//	Compaction
//
// Read path:
//
//	MemTable
//	   ↓
//	Newest SSTable
//	   ↓
//	Older SSTables
type LSM struct {
	mu sync.RWMutex

	dir string

	wal *wal.WAL

	memTable *memtable.MemTable

	sstables []*sstable.SSTable

	nextSequence uint64

	memTableLimit int
}

// Open creates or opens an LSM storage engine.
//
// Existing SSTables are loaded and the WAL is replayed
// to recover operations that were not yet flushed.
func Open(dir string, memTableLimit int) (*LSM, error) {
	if memTableLimit <= 0 {
		memTableLimit = DefaultMemTableLimit
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create LSM directory: %w", err)
	}

	walPath := filepath.Join(dir, "wal.log")

	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	l := &LSM{
		dir:           dir,
		wal:           w,
		memTable:      memtable.New(),
		sstables:      make([]*sstable.SSTable, 0),
		nextSequence:  0,
		memTableLimit: memTableLimit,
	}

	// Load previously created SSTables.
	if err := l.loadSSTables(); err != nil {
		_ = w.Close()

		return nil, fmt.Errorf(
			"load SSTables: %w",
			err,
		)
	}

	// Recover operations from WAL.
	if err := l.recoverWAL(); err != nil {
		_ = w.Close()

		return nil, fmt.Errorf(
			"recover WAL: %w",
			err,
		)
	}

	return l, nil
}

// Put inserts or updates a key.
func (l *LSM) Put(key string, value string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.nextSequence++

	operation := wal.Operation{
		Type:  "PUT",
		Key:   key,
		Value: value,
	}

	if err := l.wal.Append(operation); err != nil {
		l.nextSequence--

		return fmt.Errorf(
			"append PUT to WAL: %w",
			err,
		)
	}

	l.memTable.Put(
		key,
		value,
		l.nextSequence,
	)

	return l.flushIfNeeded()
}

// Delete logically deletes a key by creating a tombstone.
func (l *LSM) Delete(key string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.nextSequence++

	operation := wal.Operation{
		Type:  "DELETE",
		Key:   key,
		Value: "",
	}

	if err := l.wal.Append(operation); err != nil {
		l.nextSequence--

		return fmt.Errorf(
			"append DELETE to WAL: %w",
			err,
		)
	}

	l.memTable.Delete(
		key,
		l.nextSequence,
	)

	return l.flushIfNeeded()
}

// Get returns the newest value associated with a key.
func (l *LSM) Get(key string) (string, error) {
	if key == "" {
		return "", errors.New("key cannot be empty")
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	// ------------------------------------------------
	// 1. Search MemTable
	// ------------------------------------------------

	entry, err := l.memTable.Get(key)

	if err == nil {
		if entry.Tombstone {
			return "", ErrKeyNotFound
		}

		return entry.Value, nil
	}

	if !errors.Is(err, memtable.ErrKeyNotFound) {
		return "", err
	}

	// ------------------------------------------------
	// 2. Search SSTables from newest to oldest
	// ------------------------------------------------

	for i := len(l.sstables) - 1; i >= 0; i-- {
		entry, err := l.sstables[i].Get(key)

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
func (l *LSM) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.flush()
}

// flush writes the MemTable to disk.
//
// The caller must hold l.mu.
func (l *LSM) flush() error {
	entries := l.memTable.Entries()

	if len(entries) == 0 {
		return nil
	}

	path := filepath.Join(
		l.dir,
		fmt.Sprintf(
			"sstable-%020d.sst",
			l.nextSequence,
		),
	)

	_, err := sstable.Write(
		path,
		entries,
	)

	if err != nil {
		return fmt.Errorf(
			"write SSTable: %w",
			err,
		)
	}

	table := sstable.Open(path)

	l.sstables = append(
		l.sstables,
		table,
	)

	l.memTable.Clear()

	return nil
}

// flushIfNeeded automatically flushes the MemTable when
// its size reaches the configured limit.
//
// The caller must hold l.mu.
func (l *LSM) flushIfNeeded() error {
	if l.memTable.Size() < l.memTableLimit {
		return nil
	}

	return l.flush()
}

// Compact merges all SSTables into one SSTable.
func (l *LSM) Compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.sstables) <= 1 {
		return nil
	}

	allEntries := make([]memtable.Entry, 0)

	for _, table := range l.sstables {
		entries, err := readAllEntries(
			table.Path(),
		)

		if err != nil {
			return fmt.Errorf(
				"read SSTable during compaction: %w",
				err,
			)
		}

		allEntries = append(
			allEntries,
			entries...,
		)
	}

	merged := compaction.Merge(
		allEntries,
	)

	path := filepath.Join(
		l.dir,
		fmt.Sprintf(
			"compacted-%020d.sst",
			l.nextSequence,
		),
	)

	_, err := sstable.Write(
		path,
		merged,
	)

	if err != nil {
		return fmt.Errorf(
			"write compacted SSTable: %w",
			err,
		)
	}

	newTable := sstable.Open(path)

	// Remove old SSTables.
	for _, table := range l.sstables {
		if err := os.Remove(table.Path()); err != nil {
			return fmt.Errorf(
				"remove old SSTable %s: %w",
				table.Path(),
				err,
			)
		}
	}

	l.sstables = []*sstable.SSTable{
		newTable,
	}

	return nil
}

// Close safely closes the LSM engine.
func (l *LSM) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Flush remaining MemTable data.
	if err := l.flush(); err != nil {
		return err
	}

	if err := l.wal.Close(); err != nil {
		return fmt.Errorf(
			"close WAL: %w",
			err,
		)
	}

	return nil
}

// MemTableSize returns the number of entries in the
// current MemTable.
func (l *LSM) MemTableSize() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.memTable.Size()
}

// SSTableCount returns the number of SSTables.
func (l *LSM) SSTableCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return len(l.sstables)
}

// Sequence returns the current sequence number.
func (l *LSM) Sequence() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.nextSequence
}

// loadSSTables discovers existing SSTable files.
func (l *LSM) loadSSTables() error {
	entries, err := os.ReadDir(l.dir)

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

		if !strings.HasSuffix(
			name,
			".sst",
		) {
			continue
		}

		seq, err := extractSequence(name)

		if err != nil {
			continue
		}

		tables = append(
			tables,
			tableInfo{
				path: filepath.Join(
					l.dir,
					name,
				),
				seq: seq,
			},
		)

		if seq > l.nextSequence {
			l.nextSequence = seq
		}
	}

	// Oldest → newest.
	sort.Slice(
		tables,
		func(i, j int) bool {
			return tables[i].seq < tables[j].seq
		},
	)

	for _, table := range tables {
		l.sstables = append(
			l.sstables,
			sstable.Open(table.path),
		)
	}

	return nil
}

// recoverWAL replays WAL operations into the MemTable.
func (l *LSM) recoverWAL() error {
	operations, err := l.wal.Replay()

	if err != nil {
		return err
	}

	for _, operation := range operations {
		l.nextSequence++

		switch operation.Type {

		case "PUT":
			l.memTable.Put(
				operation.Key,
				operation.Value,
				l.nextSequence,
			)

		case "DELETE":
			l.memTable.Delete(
				operation.Key,
				l.nextSequence,
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
// This function is primarily used during compaction.
func readAllEntries(path string) ([]memtable.Entry, error) {
	data, err := os.ReadFile(path)

	if err != nil {
		return nil, err
	}

	content := strings.TrimSpace(
		string(data),
	)

	if content == "" {
		return nil, nil
	}

	lines := strings.Split(
		content,
		"\n",
	)

	entries := make(
		[]memtable.Entry,
		0,
		len(lines),
	)

	for _, line := range lines {
		parts := strings.SplitN(
			line,
			"|",
			4,
		)

		if len(parts) != 4 {
			return nil, fmt.Errorf(
				"invalid SSTable record: %q",
				line,
			)
		}

		tombstone, err := strconv.ParseBool(
			parts[2],
		)

		if err != nil {
			return nil, fmt.Errorf(
				"invalid tombstone value: %q",
				parts[2],
			)
		}

		sequence, err := strconv.ParseUint(
			parts[3],
			10,
			64,
		)

		if err != nil {
			return nil, fmt.Errorf(
				"invalid sequence number: %q",
				parts[3],
			)
		}

		entries = append(
			entries,
			memtable.Entry{
				Key:       parts[0],
				Value:     parts[1],
				Tombstone: tombstone,
				Sequence:  sequence,
			},
		)
	}

	return entries, nil
}

// extractSequence extracts the sequence number from
// SSTable filenames.
//
// Example:
//
//	sstable-00000000000000000005.sst
//
// returns:
//
//	5
func extractSequence(name string) (uint64, error) {
	name = strings.TrimSuffix(
		name,
		".sst",
	)

	index := strings.LastIndex(
		name,
		"-",
	)

	if index == -1 {
		return 0, errors.New(
			"invalid SSTable filename",
		)
	}

	sequenceString := name[index+1:]

	return strconv.ParseUint(
		sequenceString,
		10,
		64,
	)
}

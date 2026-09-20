package sstable

import (
	"path/filepath"
	"testing"

	"github.com/venkatanish04/tasksync/internal/lsm/memtable"
)

func TestWriteAndGet(t *testing.T) {
	tempDir := t.TempDir()

	path := filepath.Join(
		tempDir,
		"sstable-000001",
	)

	entries := []memtable.Entry{
		{
			Key:       "task/101",
			Value:     "Learn Go",
			Tombstone: false,
			Sequence:  1,
		},
		{
			Key:       "task/102",
			Value:     "Learn Raft",
			Tombstone: false,
			Sequence:  2,
		},
		{
			Key:       "task/103",
			Value:     "Learn gRPC",
			Tombstone: false,
			Sequence:  3,
		},
	}

	table, err := Write(path, entries)

	if err != nil {
		t.Fatalf(
			"failed to write SSTable: %v",
			err,
		)
	}

	if table.Path() != path {
		t.Fatalf(
			"expected path %s, got %s",
			path,
			table.Path(),
		)
	}

	entry, err := table.Get("task/102")

	if err != nil {
		t.Fatalf(
			"failed to get entry: %v",
			err,
		)
	}

	if entry.Value != "Learn Raft" {
		t.Fatalf(
			"expected %q, got %q",
			"Learn Raft",
			entry.Value,
		)
	}

	if entry.Sequence != 2 {
		t.Fatalf(
			"expected sequence 2, got %d",
			entry.Sequence,
		)
	}
}

func TestGetMissingKey(t *testing.T) {
	tempDir := t.TempDir()

	path := filepath.Join(
		tempDir,
		"sstable-000002",
	)

	entries := []memtable.Entry{
		{
			Key:      "task/101",
			Value:    "Learn Go",
			Sequence: 1,
		},
	}

	table, err := Write(path, entries)

	if err != nil {
		t.Fatalf(
			"failed to write SSTable: %v",
			err,
		)
	}

	_, err = table.Get("task/999")

	if err != ErrKeyNotFound {
		t.Fatalf(
			"expected ErrKeyNotFound, got %v",
			err,
		)
	}
}

func TestTombstone(t *testing.T) {
	tempDir := t.TempDir()

	path := filepath.Join(
		tempDir,
		"sstable-000003",
	)

	entries := []memtable.Entry{
		{
			Key:       "task/101",
			Value:     "",
			Tombstone: true,
			Sequence:  3,
		},
	}

	table, err := Write(path, entries)

	if err != nil {
		t.Fatalf(
			"failed to write SSTable: %v",
			err,
		)
	}

	entry, err := table.Get("task/101")

	if err != nil {
		t.Fatalf(
			"failed to get tombstone: %v",
			err,
		)
	}

	if !entry.Tombstone {
		t.Fatal(
			"expected tombstone to be true",
		)
	}

	if entry.Sequence != 3 {
		t.Fatalf(
			"expected sequence 3, got %d",
			entry.Sequence,
		)
	}
}

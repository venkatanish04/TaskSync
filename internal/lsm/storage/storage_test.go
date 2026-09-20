package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestStorage(t *testing.T, memTableLimit int) (*Storage, string) {
	t.Helper()

	dir := t.TempDir()

	store, err := Open(dir, memTableLimit)
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}

	return store, dir
}

func TestPutAndGet(t *testing.T) {
	store, _ := newTestStorage(t, 10)
	defer store.Close()

	err := store.Put("task/1", "Learn Go")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	value, err := store.Get("task/1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if value != "Learn Go" {
		t.Fatalf(
			"expected %q, got %q",
			"Learn Go",
			value,
		)
	}
}

func TestUpdate(t *testing.T) {
	store, _ := newTestStorage(t, 10)
	defer store.Close()

	if err := store.Put("task/1", "Learn Go"); err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/1", "Learn Raft"); err != nil {
		t.Fatal(err)
	}

	value, err := store.Get("task/1")
	if err != nil {
		t.Fatal(err)
	}

	if value != "Learn Raft" {
		t.Fatalf(
			"expected updated value %q, got %q",
			"Learn Raft",
			value,
		)
	}
}

func TestDelete(t *testing.T) {
	store, _ := newTestStorage(t, 10)
	defer store.Close()

	if err := store.Put("task/1", "Learn Go"); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete("task/1"); err != nil {
		t.Fatal(err)
	}

	_, err := store.Get("task/1")

	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf(
			"expected ErrKeyNotFound, got %v",
			err,
		)
	}
}

func TestMissingKey(t *testing.T) {
	store, _ := newTestStorage(t, 10)
	defer store.Close()

	_, err := store.Get("does-not-exist")

	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf(
			"expected ErrKeyNotFound, got %v",
			err,
		)
	}
}

func TestAutomaticFlush(t *testing.T) {
	store, _ := newTestStorage(t, 2)
	defer store.Close()

	if err := store.Put("task/1", "A"); err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/2", "B"); err != nil {
		t.Fatal(err)
	}

	if store.SSTableCount() != 1 {
		t.Fatalf(
			"expected 1 SSTable, got %d",
			store.SSTableCount(),
		)
	}

	if store.MemTableSize() != 0 {
		t.Fatalf(
			"expected empty MemTable, got %d",
			store.MemTableSize(),
		)
	}

	value, err := store.Get("task/1")
	if err != nil {
		t.Fatal(err)
	}

	if value != "A" {
		t.Fatalf(
			"expected %q, got %q",
			"A",
			value,
		)
	}
}

func TestGetFromSSTable(t *testing.T) {
	store, _ := newTestStorage(t, 2)
	defer store.Close()

	if err := store.Put("task/1", "A"); err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/2", "B"); err != nil {
		t.Fatal(err)
	}

	// MemTable should have been flushed.
	if store.MemTableSize() != 0 {
		t.Fatalf("MemTable should be empty after flush")
	}

	value, err := store.Get("task/1")
	if err != nil {
		t.Fatal(err)
	}

	if value != "A" {
		t.Fatalf(
			"expected %q, got %q",
			"A",
			value,
		)
	}
}

func TestDeleteAcrossSSTables(t *testing.T) {
	store, _ := newTestStorage(t, 2)
	defer store.Close()

	// First SSTable.
	if err := store.Put("task/1", "A"); err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/2", "B"); err != nil {
		t.Fatal(err)
	}

	// Second SSTable.
	if err := store.Delete("task/1"); err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/3", "C"); err != nil {
		t.Fatal(err)
	}

	_, err := store.Get("task/1")

	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf(
			"expected deleted key to be unavailable, got %v",
			err,
		)
	}

	value, err := store.Get("task/2")
	if err != nil {
		t.Fatal(err)
	}

	if value != "B" {
		t.Fatalf(
			"expected task/2 to be B, got %q",
			value,
		)
	}
}

func TestCompaction(t *testing.T) {
	store, dir := newTestStorage(t, 2)
	defer store.Close()

	// SSTable 1
	if err := store.Put("task/1", "A"); err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/2", "B"); err != nil {
		t.Fatal(err)
	}

	// SSTable 2
	if err := store.Put("task/1", "A-updated"); err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/3", "C"); err != nil {
		t.Fatal(err)
	}

	if store.SSTableCount() != 2 {
		t.Fatalf(
			"expected 2 SSTables, got %d",
			store.SSTableCount(),
		)
	}

	err := store.Compact()
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	if store.SSTableCount() != 1 {
		t.Fatalf(
			"expected 1 SSTable after compaction, got %d",
			store.SSTableCount(),
		)
	}

	value, err := store.Get("task/1")
	if err != nil {
		t.Fatal(err)
	}

	if value != "A-updated" {
		t.Fatalf(
			"expected newest value %q, got %q",
			"A-updated",
			value,
		)
	}

	value, err = store.Get("task/2")
	if err != nil {
		t.Fatal(err)
	}

	if value != "B" {
		t.Fatalf(
			"expected task/2 to be B, got %q",
			value,
		)
	}

	value, err = store.Get("task/3")
	if err != nil {
		t.Fatal(err)
	}

	if value != "C" {
		t.Fatalf(
			"expected task/3 to be C, got %q",
			value,
		)
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}

	sstCount := 0

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sst" {
			sstCount++
		}
	}

	if sstCount != 1 {
		t.Fatalf(
			"expected 1 SSTable file, found %d",
			sstCount,
		)
	}
}

func TestCloseFlushesMemTable(t *testing.T) {
	dir := t.TempDir()

	store, err := Open(dir, 100)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/1", "A"); err != nil {
		t.Fatal(err)
	}

	if store.MemTableSize() != 1 {
		t.Fatalf("expected one MemTable entry")
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen storage.
	store2, err := Open(dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	value, err := store2.Get("task/1")
	if err != nil {
		t.Fatal(err)
	}

	if value != "A" {
		t.Fatalf(
			"expected %q after reopen, got %q",
			"A",
			value,
		)
	}
}

func TestSequenceNumbers(t *testing.T) {
	store, _ := newTestStorage(t, 100)
	defer store.Close()

	if err := store.Put("task/1", "A"); err != nil {
		t.Fatal(err)
	}

	if err := store.Put("task/2", "B"); err != nil {
		t.Fatal(err)
	}

	if err := store.Delete("task/1"); err != nil {
		t.Fatal(err)
	}

	if store.Sequence() != 3 {
		t.Fatalf(
			"expected sequence 3, got %d",
			store.Sequence(),
		)
	}
}

func TestConcurrentAccess(t *testing.T) {
	store, _ := newTestStorage(t, 1000)
	defer store.Close()

	done := make(chan bool, 100)

	for i := 0; i < 100; i++ {
		go func(index int) {
			key := "task/" + string(rune(index))

			if err := store.Put(key, "value"); err != nil {
				t.Errorf("Put failed: %v", err)
			}

			_, err := store.Get(key)

			if err != nil {
				t.Errorf("Get failed: %v", err)
			}

			done <- true
		}(i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}
}

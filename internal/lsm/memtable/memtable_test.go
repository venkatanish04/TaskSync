package memtable

import "testing"

func TestPutAndGet(t *testing.T) {
	m := New()

	m.Put("task/101", "Learn Go", 1)

	entry, err := m.Get("task/101")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.Value != "Learn Go" {
		t.Fatalf(
			"expected value %q, got %q",
			"Learn Go",
			entry.Value,
		)
	}

	if entry.Sequence != 1 {
		t.Fatalf(
			"expected sequence 1, got %d",
			entry.Sequence,
		)
	}

	if entry.Tombstone {
		t.Fatal("entry should not be a tombstone")
	}
}

func TestUpdate(t *testing.T) {
	m := New()

	m.Put("task/101", "Learn Go", 1)
	m.Put("task/101", "Learn Raft", 2)

	entry, err := m.Get("task/101")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.Value != "Learn Raft" {
		t.Fatalf(
			"expected updated value %q, got %q",
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

func TestDelete(t *testing.T) {
	m := New()

	m.Put("task/101", "Learn Go", 1)
	m.Delete("task/101", 2)

	entry, err := m.Get("task/101")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !entry.Tombstone {
		t.Fatal("expected tombstone after delete")
	}

	if entry.Sequence != 2 {
		t.Fatalf(
			"expected sequence 2, got %d",
			entry.Sequence,
		)
	}
}

func TestSize(t *testing.T) {
	m := New()

	m.Put("task/101", "Learn Go", 1)
	m.Put("task/102", "Learn Raft", 2)

	if m.Size() != 2 {
		t.Fatalf(
			"expected size 2, got %d",
			m.Size(),
		)
	}

	m.Put("task/101", "Learn gRPC", 3)

	if m.Size() != 2 {
		t.Fatalf(
			"expected size to remain 2 after update, got %d",
			m.Size(),
		)
	}
}

func TestMissingKey(t *testing.T) {
	m := New()

	_, err := m.Get("task/999")

	if err != ErrKeyNotFound {
		t.Fatalf(
			"expected ErrKeyNotFound, got %v",
			err,
		)
	}
}

func TestEntriesSorted(t *testing.T) {
	m := New()

	m.Put("task/103", "Learn gRPC", 3)
	m.Put("task/101", "Learn Go", 1)
	m.Put("task/102", "Learn Raft", 2)

	entries := m.Entries()

	if len(entries) != 3 {
		t.Fatalf(
			"expected 3 entries, got %d",
			len(entries),
		)
	}

	expected := []string{
		"task/101",
		"task/102",
		"task/103",
	}

	for i, entry := range entries {
		if entry.Key != expected[i] {
			t.Fatalf(
				"expected key %s at position %d, got %s",
				expected[i],
				i,
				entry.Key,
			)
		}
	}
}

func TestClear(t *testing.T) {
	m := New()

	m.Put("task/101", "Learn Go", 1)
	m.Put("task/102", "Learn Raft", 2)

	m.Clear()

	if m.Size() != 0 {
		t.Fatalf(
			"expected size 0 after clear, got %d",
			m.Size(),
		)
	}
}

func TestConcurrentAccess(t *testing.T) {
	m := New()

	done := make(chan bool, 20)

	for i := 0; i < 10; i++ {
		go func(id int) {
			key := "task/" + string(rune('A'+id))

			m.Put(key, "running", uint64(id+1))

			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		go func(id int) {
			key := "task/" + string(rune('A'+id))

			_, _ = m.Get(key)

			done <- true
		}(i)
	}

	for i := 0; i < 20; i++ {
		<-done
	}
}

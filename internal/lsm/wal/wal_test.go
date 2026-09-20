package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReplay(t *testing.T) {
	tempDir := t.TempDir()

	walPath := filepath.Join(tempDir, "tasksync.wal")

	w, err := Open(walPath)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	defer w.Close()

	operations := []Operation{
		{
			Type:  "PUT",
			Key:   "task/101",
			Value: "Learn Go",
		},
		{
			Type:  "PUT",
			Key:   "task/102",
			Value: "Learn Raft",
		},
		{
			Type:  "DELETE",
			Key:   "task/101",
			Value: "",
		},
	}

	for _, operation := range operations {
		if err := w.Append(operation); err != nil {
			t.Fatalf("failed to append operation: %v", err)
		}
	}

	replayed, err := w.Replay()
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if len(replayed) != len(operations) {
		t.Fatalf(
			"expected %d operations, got %d",
			len(operations),
			len(replayed),
		)
	}

	for i := range operations {
		if replayed[i] != operations[i] {
			t.Errorf(
				"operation %d mismatch: expected %+v, got %+v",
				i,
				operations[i],
				replayed[i],
			)
		}
	}

	_, err = os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file does not exist: %v", err)
	}
}

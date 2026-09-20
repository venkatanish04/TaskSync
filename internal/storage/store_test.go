package storage

import (
	"testing"

	"github.com/venkatanish04/tasksync/internal/task"
)

func TestPutAndGet(t *testing.T) {
	store := NewStore()

	input := task.Task{
		ID:     "101",
		Title:  "Learn Go",
		Status: "pending",
	}

	store.Put(input)

	result, err := store.Get("101")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ID != input.ID {
		t.Errorf("expected ID %s, got %s", input.ID, result.ID)
	}

	if result.Title != input.Title {
		t.Errorf("expected title %s, got %s", input.Title, result.Title)
	}
}
func TestDelete(t *testing.T) {
	store := NewStore()

	input := task.Task{
		ID:     "102",
		Title:  "Learn Raft",
		Status: "pending",
	}

	store.Put(input)

	err := store.Delete("102")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = store.Get("102")

	if err == nil {
		t.Fatal("expected task to be deleted")
	}
}

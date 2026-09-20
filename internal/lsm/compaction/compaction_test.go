package compaction

import (
	"testing"

	"github.com/venkatanish04/tasksync/internal/lsm/memtable"
)

func TestMergeKeepsNewestVersion(t *testing.T) {

	entries := []memtable.Entry{
		{
			Key:      "task/101",
			Value:    "Learn Go",
			Sequence: 1,
		},
		{
			Key:      "task/101",
			Value:    "Learn Raft",
			Sequence: 3,
		},
		{
			Key:      "task/102",
			Value:    "Learn Python",
			Sequence: 2,
		},
		{
			Key:      "task/102",
			Value:    "Learn Java",
			Sequence: 4,
		},
	}

	result := Merge(entries)

	if len(result) != 2 {
		t.Fatalf(
			"expected 2 entries, got %d",
			len(result),
		)
	}

	if result[0].Key != "task/101" {
		t.Fatalf(
			"expected task/101, got %s",
			result[0].Key,
		)
	}

	if result[0].Value != "Learn Raft" {
		t.Fatalf(
			"expected Learn Raft, got %s",
			result[0].Value,
		)
	}

	if result[0].Sequence != 3 {
		t.Fatalf(
			"expected sequence 3, got %d",
			result[0].Sequence,
		)
	}

	if result[1].Key != "task/102" {
		t.Fatalf(
			"expected task/102, got %s",
			result[1].Key,
		)
	}

	if result[1].Value != "Learn Java" {
		t.Fatalf(
			"expected Learn Java, got %s",
			result[1].Value,
		)
	}

	if result[1].Sequence != 4 {
		t.Fatalf(
			"expected sequence 4, got %d",
			result[1].Sequence,
		)
	}
}

func TestMergeKeepsNewestTombstone(t *testing.T) {

	entries := []memtable.Entry{
		{
			Key:       "task/101",
			Value:     "Learn Go",
			Tombstone: false,
			Sequence:  1,
		},
		{
			Key:       "task/101",
			Value:     "",
			Tombstone: true,
			Sequence:  2,
		},
	}

	result := Merge(entries)

	if len(result) != 1 {
		t.Fatalf(
			"expected 1 entry, got %d",
			len(result),
		)
	}

	if !result[0].Tombstone {
		t.Fatal(
			"expected newest entry to be tombstone",
		)
	}

	if result[0].Sequence != 2 {
		t.Fatalf(
			"expected sequence 2, got %d",
			result[0].Sequence,
		)
	}
}

func TestMergeEmpty(t *testing.T) {

	result := Merge(nil)

	if result != nil {
		t.Fatalf(
			"expected nil result, got %v",
			result,
		)
	}
}

func TestMergeDifferentKeys(t *testing.T) {

	entries := []memtable.Entry{
		{
			Key:      "task/103",
			Value:    "Learn gRPC",
			Sequence: 3,
		},
		{
			Key:      "task/101",
			Value:    "Learn Go",
			Sequence: 1,
		},
		{
			Key:      "task/102",
			Value:    "Learn Raft",
			Sequence: 2,
		},
	}

	result := Merge(entries)

	expectedKeys := []string{
		"task/101",
		"task/102",
		"task/103",
	}

	if len(result) != len(expectedKeys) {
		t.Fatalf(
			"expected %d entries, got %d",
			len(expectedKeys),
			len(result),
		)
	}

	for i, expected := range expectedKeys {
		if result[i].Key != expected {
			t.Fatalf(
				"expected key %s at position %d, got %s",
				expected,
				i,
				result[i].Key,
			)
		}
	}
}

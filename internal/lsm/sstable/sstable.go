package sstable

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/venkatanish04/tasksync/internal/lsm/memtable"
)

var ErrKeyNotFound = errors.New("key not found")

type SSTable struct {
	path string
}

func Write(path string, entries []memtable.Entry) (*SSTable, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writer := bufio.NewWriter(file)

	for _, entry := range entries {
		line := fmt.Sprintf(
			"%s|%s|%t|%d\n",
			entry.Key,
			entry.Value,
			entry.Tombstone,
			entry.Sequence,
		)

		if _, err := writer.WriteString(line); err != nil {
			file.Close()
			return nil, err
		}
	}

	if err := writer.Flush(); err != nil {
		file.Close()
		return nil, err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return nil, err
	}

	if err := file.Close(); err != nil {
		return nil, err
	}

	return &SSTable{
		path: path,
	}, nil
}

func Open(path string) *SSTable {
	return &SSTable{
		path: path,
	}
}

func (s *SSTable) Get(key string) (memtable.Entry, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return memtable.Entry{}, err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "|", 4)

		if len(parts) != 4 {
			return memtable.Entry{}, fmt.Errorf(
				"invalid SSTable record: %q",
				scanner.Text(),
			)
		}

		if parts[0] != key {
			continue
		}

		tombstone, err := strconv.ParseBool(parts[2])
		if err != nil {
			return memtable.Entry{}, fmt.Errorf(
				"invalid tombstone value: %q",
				parts[2],
			)
		}

		sequence, err := strconv.ParseUint(parts[3], 10, 64)
		if err != nil {
			return memtable.Entry{}, fmt.Errorf(
				"invalid sequence number: %q",
				parts[3],
			)
		}

		return memtable.Entry{
			Key:       parts[0],
			Value:     parts[1],
			Tombstone: tombstone,
			Sequence:  sequence,
		}, nil
	}

	if err := scanner.Err(); err != nil {
		return memtable.Entry{}, err
	}

	return memtable.Entry{}, ErrKeyNotFound
}

func (s *SSTable) Path() string {
	return s.path
}

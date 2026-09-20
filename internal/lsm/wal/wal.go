package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type Operation struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
}

func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

func (w *WAL) Append(operation Operation) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(operation)
	if err != nil {
		return err
	}

	data = append(data, '\n')

	_, err = w.file.Write(data)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Replay() ([]Operation, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	var operations []Operation

	scanner := bufio.NewScanner(w.file)

	for scanner.Scan() {
		var operation Operation

		if err := json.Unmarshal(scanner.Bytes(), &operation); err != nil {
			return nil, err
		}

		operations = append(operations, operation)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return operations, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}

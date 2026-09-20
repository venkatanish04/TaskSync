package storage

import (
	"errors"
	"sync"

	"github.com/venkatanish04/tasksync/internal/task"
)

type Store struct {
	mu    sync.RWMutex
	tasks map[string]task.Task
}

func NewStore() *Store {
	return &Store{
		tasks: make(map[string]task.Task),
	}
}

func (s *Store) Put(t task.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks[t.ID] = t
}

func (s *Store) Get(id string) (task.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, exists := s.tasks[id]

	if !exists {
		return task.Task{}, errors.New("task not found")
	}

	return t, nil
}

func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[id]; !exists {
		return errors.New("task not found")
	}

	delete(s.tasks, id)

	return nil
}

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.tasks)
}

package btree

import (
	"errors"
	"math/rand"
)

// ErrCatchAll use this error wherever the actual error to be thrown is not decided till now
var ErrCatchAll = errors.New("CatchAll")

type StorageManager interface {
	PageSize() int
	Alloc() (PageId, Bytes, error)
	Exists(id PageId) bool
	Write(id PageId, data []byte) error
	Read(id PageId) ([]byte, bool)
	Delete(id PageId) error
}

// Naive in-memory storage manager
var sm StorageManager

func init() {
	sm = &MemoryStorage{make(map[PageId]Bytes)}
}

type MemoryStorage struct {
	data map[PageId]Bytes
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{make(map[PageId]Bytes)}
}
func (s *MemoryStorage) PageSize() int {
	return PageSize
}

func (s *MemoryStorage) Exists(id PageId) bool {
	_, ok := s.data[id]
	return ok
}

func (s *MemoryStorage) Read(id PageId) ([]byte, bool) {
	if !s.Exists(id) {
		return nil, false
	}

	b, ok := s.data[id]
	return b, ok
}

func (s *MemoryStorage) Alloc() (PageId, Bytes, error) {
	id := rand.Uint32()
	_, ok := s.data[id]
	for ok {
		id = rand.Uint32()
		_, ok = s.data[id]
	}

	s.data[id] = make([]byte, s.PageSize())
	return id, s.data[id], nil
}

func (s *MemoryStorage) Write(id PageId, data []byte) error {
	if !s.Exists(id) || len(s.data[id]) != s.PageSize() {
		return ErrCatchAll
	}

	s.data[id] = data
	return nil
}

func (s *MemoryStorage) Delete(id PageId) error {
	if !s.Exists(id) {
		return ErrCatchAll
	}
	delete(s.data, id)
	return nil
}

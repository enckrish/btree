package btree

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestMemoryStorageAlloc(t *testing.T) {
	sm := NewMemoryStorage()

	id, pg, err := sm.Alloc()
	if err != nil {
		t.Fatal(err)
	}

	if id == 0 {
		t.Fatal("id should not be zero")
	}

	if len(pg) != sm.PageSize() {
		t.Fatal("size should be equal to page size")
	}
}

func TestMemoryStorageExists(t *testing.T) {
	sm := NewMemoryStorage()
	const cases = 10000

	ids := make([]PageId, 0, cases)
	for range cases {
		id, _, _ := sm.Alloc()
		ids = append(ids, id)
	}

	for _, id := range ids {
		if !sm.Exists(id) {
			t.Fatal("id should exist")
		}
	}
}

func TestMemoryStorageReadWrite(t *testing.T) {
	sm := NewMemoryStorage()
	id, pg, _ := sm.Alloc()

	_, _ = rand.Read(pg)
	err := sm.Write(id, pg)
	if err != nil {
		t.Fatal(err)
	}

	rd, ok := sm.Read(id)
	if !ok {
		t.Fatal("id should exist")
	}

	if !bytes.Equal(rd, pg) {
		t.Fatal("read value should be equal to pg")
	}
}

func TestMemoryStorageDelete(t *testing.T) {
	sm := NewMemoryStorage()
	id, _, _ := sm.Alloc()

	assertErr(sm.Delete(id) == nil, "delete should not return error")
	assertErr(!sm.Exists(id), "id should not exist after deletion")
}

func TestMemoryStorageFalseId(t *testing.T) {
	sm := NewMemoryStorage()

	id := PageId(42) // arbitrary id that does not exist
	_, ok := sm.Read(id)
	assertErr(!ok, "id should not exist")

	err := sm.Write(id, []byte{1, 2, 3})
	assertErr(err != nil, "should return error when writing to non-existing id")

	err = sm.Delete(id)
	assertErr(err != nil, "should return error when deleting non-existing id")

	assertErr(!sm.Exists(id), "id should not exist")
}

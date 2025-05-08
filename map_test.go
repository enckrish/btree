package btree

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
)

// buildComparableMaps returns a B+ Tree map, a go native map built on the same data, and their keys in sorted order
func buildComparableMaps(nkeys, degree int) (*Map[Hash, int], map[Hash]int, []Hash) {
	m := NewMap[Hash, int](degree, func(s Hash) Hash {
		return s
	})
	goMap := map[Hash]int{}
	keys, values := GetData(nkeys)

	// Pushing values in native map and b+ tree map
	for i := 0; i < nkeys; i++ {
		m.Set(keys[i], &values[i])
		goMap[keys[i]] = values[i]
	}

	// sorting the keys
	slices.SortFunc(keys, func(a, b Hash) int {
		return bytes.Compare(a[:], b[:])
	})
	return m, goMap, keys
}

// Test that implementation contains all valid keys->value pairings; it doesn't check if invalid pairs exist too
// for that the number of such pairs are counted in TestMapKeyCount
func TestMapCorrectMappings(t *testing.T) {
	m, goMap, _ := buildComparableMaps(1000, 3)

	// Checking if all key-value pairs are maintained
	for k, v := range goMap {
		mvp := m.Get(k)
		if mvp == nil {
			t.Fatalf("%s not exist", k)
		} else if *mvp != v {
			t.Fatalf("got %d, want %d", *mvp, v)
		}
	}
}

// Verify that num keys in map is equal to keys actually entered
func TestMapKeyCount(t *testing.T) {
	m, _, keys := buildComparableMaps(2000, 5)
	nkeys := len(keys)

	// leftmost leaf
	l, _ := m.root.lbPositionedRef(nil)

	// `keysCounted` should match nkeys after loop
	keysCounted := 0
	for l != nil {
		keysCounted += len(l.keys)
		l = l.next
	}

	if keysCounted != nkeys {
		t.Fatalf("Expected %d keys, got %d", nkeys, keysCounted)
	}
}

// Nodes that follow all B+ Tree criteria are healthy, unhealthy nodes must be equal to 0
func TestMapHealthy(t *testing.T) {
	m, _, _ := buildComparableMaps(2000, 10)
	un, to := m.root.numUnhealthyChildren()
	if un != 0 {
		t.Fatalf("unhealthy children ratio = %d/%d", un, to)
	}
}

// A B+ tree with degree d and keys k must have max height = floor(log(x = floor(k / ceil(d-1/2)), base = ceil(d/2)))
func TestMapMaxHeight(t *testing.T) {
	m, _, keys := buildComparableMaps(2000, 3)
	nkeys := len(keys)

	minLeafKeys := ceilDiv(m.deg-1, 2)
	maxLeaves := nkeys / minLeafKeys // no ceiling
	minPointers := ceilDiv(m.deg, 2)
	expectedMaxHeight := int(logBase(maxLeaves, minPointers)) // no ceiling

	if m.height > expectedMaxHeight {
		t.Fatalf("With nkeys: %d and degree: %d, expected max height of %d, got %d", nkeys, m.deg, expectedMaxHeight, m.height)
	}
}

// Empty maps shouldn't iter on any values when using map.All
func TestMapEmptyIter(t *testing.T) {
	m := NewMap[Hash, int](3, func(s Hash) Hash {
		return s
	})
	for range m.All() {
		t.Fatalf("iterations shouldn't have run on an empty map")
	}
}

// Test that map.All(0 iterates over all keys in sorted order
func TestMapAllIterator(t *testing.T) {
	m, _, keys := buildComparableMaps(200, 3)
	nkeys := len(keys)
	fmt.Println("Height:", m.height)

	// `keysCounted` should match nkeys after loop
	keysCounted := 0
	for range m.All() {
		keysCounted++
	}

	if keysCounted != nkeys {
		t.Errorf("Expected %d iterations, got %d", nkeys, keysCounted)
	}
}

func BenchmarkTreeSet(b *testing.B) {
	m := NewMap[Hash, int](8, func(s Hash) Hash {
		return s
	})
	keys, values := GetData(b.N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		m.Set(keys[i], &values[i])
	}
}

func BenchmarkGoMapSet(b *testing.B) {
	m := make(map[Hash]int)
	keys, values := GetData(b.N)
	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		m[keys[i]] = values[i] // value doesn't matter
	}
}

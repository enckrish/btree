package btree

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"
)

// buildComparableMaps returns a B+ Tree map, a go native map built on the same data, and their keys in sorted order
func buildComparableMaps(nKeys, degree int) (*Map[Hash, int], map[Hash]int, []Hash) {
	m := NewMap[Hash, int](degree, func(s Hash) Hash {
		return s
	}, maxPermissibleMapHeight(nKeys, degree))
	goMap := map[Hash]int{}
	keys, values := GetData(nKeys)

	// Pushing values in native map and b+ tree map
	for i := 0; i < nKeys; i++ {
		m.Set(keys[i], &values[i])
		goMap[keys[i]] = values[i]
	}

	// sorting the keys
	slices.SortFunc(keys, func(a, b Hash) int {
		return bytes.Compare(a[:], b[:])
	})
	return m, goMap, keys
}

// A B+ tree with degree d and keys k must have max height = floor(log(x = floor(k / ceil(d-1/2)), base = ceil(d/2)))
func maxPermissibleMapHeight(nKeys int, degree int) int {
	if nKeys < degree-1 {
		return 1
	}
	minLeafKeys := ceilDiv(degree-1, 2)
	maxLeaves := nKeys / minLeafKeys // no ceiling
	minPointers := ceilDiv(degree, 2)
	expectedMaxHeight := int(logBase(maxLeaves, minPointers)) // no ceiling
	return expectedMaxHeight + 1                              // as it didn't account for the root having less than minPointers
}

func runMapHealthTests[V any](t *testing.T, m *Map[Hash, V], nKeys int, fail bool) (passed bool) {
	fn := t.Errorf
	if fail {
		fn = t.Fatalf
	}

	// height tests
	if expectedMaxHeight := maxPermissibleMapHeight(nKeys, m.deg); m.height > expectedMaxHeight {
		fn("With nkeys: %d and degree: %d, expected max height of %d, got %d", nKeys, m.deg, expectedMaxHeight, m.height)
		return false
	}

	// node-wise health tests
	un, to := m.root.numUnhealthyChildren()
	if un != 0 {
		fn("unhealthy children ratio = %d/%d", un, to)
		return false
	}

	return true
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
	nKeys := len(keys)

	// leftmost leaf
	l, _ := lbPositionedRef(m.root, nil)

	// `keysCounted` should match nKeys after loop
	keysCounted := 0
	for l != nil {
		keysCounted += len(l.keys)
		l = l.next
	}

	if keysCounted != nKeys {
		t.Fatalf("Expected %d keys, got %d", nKeys, keysCounted)
	}
}

// Nodes that follow all B+ Tree criteria are healthy, unhealthy nodes must be equal to 0
func TestMapHealthy(t *testing.T) {
	const nKeys = 20000
	const degree = 3
	m, _, _ := buildComparableMaps(nKeys, degree)
	runMapHealthTests(t, m, nKeys, true)
}

// Empty maps shouldn't iter on any values when using map.All
func TestMapEmptyIter(t *testing.T) {
	m := NewMap[Hash, int](3, func(s Hash) Hash {
		return s
	}, 0)
	for range m.All() {
		t.Fatalf("iterations shouldn't have run on an empty map")
	}
}

func TestMapDelete(t *testing.T) {
	const nKeys = 5000
	const degree = 10
	const subsetSizeMul = 0.9

	m, _, keys := buildComparableMaps(nKeys, degree)
	runMapHealthTests(t, m, nKeys, true)
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	delKeysLen := int(float64(len(keys)) * subsetSizeMul)
	delKeys := keys[:delKeysLen]

	// try deleting
	for i, key := range delKeys {
		del := m.Del(key)
		if !del {
			t.Fatalf("deletion failed")
		}
		if !runMapHealthTests(t, m, nKeys-i-1, true) {
			t.Fatalf("iteration: %d resulted in unhealthy map", i)
		}
	}

	// check if deleted keys return false on further deletion and nil values on get op
	for _, key := range delKeys {
		del := m.Del(key)
		if del {
			t.Fatalf("deleted keys returning true on m.Del")
		}

		v := m.Get(key)
		if v != nil {
			t.Fatalf("deleted key returns non-nil value")
		}
	}

	// Check if rest keys are as it is
	for _, key := range keys[delKeysLen:] {
		v := m.Get(key)
		if v == nil {
			t.Fatalf("non-deleted key returns nil value")
		}
	}

	// Delete all and check height
	for _, key := range keys[delKeysLen:] {
		del := m.Del(key)
		if !del {
			t.Fatalf("deletion failed")
		}
	}

	if m.height != 0 {
		t.Fatalf("height should be 0, got %d", m.height)
	}

	// Check if reinsertions work
	for _, key := range keys {
		v := rand.Int()
		m.Set(key, &v)
	}
}

// Test that map.All(0 iterates over all keys in sorted order
func TestMapAllIterator(t *testing.T) {
	m, _, keys := buildComparableMaps(200, 3)
	nKeys := len(keys)

	// `keysCounted` should match nKeys after loop
	keysCounted := 0
	for range m.All() {
		keysCounted++
	}

	if keysCounted != nKeys {
		t.Errorf("Expected %d iterations, got %d", nKeys, keysCounted)
	}
}

func BenchmarkTreeSet(b *testing.B) {
	const degree = 30
	m := NewMap[Hash, int](degree, func(s Hash) Hash {
		return s
	}, maxPermissibleMapHeight(b.N, degree))
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
		m[keys[i]] = values[i]
	}
}

//
//func BenchmarkSearchFnCmp(b *testing.B) {
//	const degree = 5
//
//	preValue := lowerBoundBytesArr
//	defer func() {
//		lowerBoundBytesArr = preValue
//	}()
//
//	fns := [2]func([]Bytes, Bytes) (int, bool){lbBinSearch, lbLinSearch}
//	names := [2]string{"binSearch", "linSearch"}
//
//	for k := range fns {
//		b.Run(names[k], func(b *testing.B) {
//			lowerBoundBytesArr = fns[k]
//			keys, values := GetData(b.N)
//			m := NewMap[Hash, int](degree, func(s Hash) Hash {
//				return s
//			})
//			b.ResetTimer()
//			b.ReportAllocs()
//			for i := range b.N {
//				m.Set(keys[i], &values[i])
//			}
//		})
//	}
//}

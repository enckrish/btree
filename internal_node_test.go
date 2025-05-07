package btree

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"
)

func MakeInternalNode(deg int) *InternalNode[int] {
	in := newInternalNode[int](deg)

	rightSentLeaf := &LeafNode[int]{
		keys:   []Bytes{Bytes{250}},
		values: []int{rand.Int()},
		next:   nil,
	}

	leftSentLeaf := &LeafNode[int]{
		keys:   []Bytes{Bytes{5}},
		values: []int{rand.Int()},
		next:   rightSentLeaf,
	}
	
	in.keys = append(in.keys, Bytes{250})
	in.pointers = append(in.pointers, leftSentLeaf, rightSentLeaf)
	return in
}

func MakeKeysWithGaps(n int, gap int) []Bytes {
	if n*gap+5 >= 250 {
		panic("test misconfiguration: max custom key must be < 250")
	}
	keys := make([]Bytes, 0, n)
	for i := 1; i <= n; i++ {
		keys = append(keys, Bytes{byte(i*gap + 5)})
	}
	return keys
}

func MakeFilledNode() (*InternalNode[int], []Bytes) {
	in := MakeInternalNode(30)
	keys := MakeKeysWithGaps(28, 5)
	for i, key := range keys {
		_, newNode := in.setOrInsert(key, i*10+1)
		if newNode != nil {
			panic(fmt.Sprintf("possible test misconfiguration: at iter %d, more keys than can fit in single node", i))
		}
	}
	if len(in.pointers) != 29 && len(in.pointers) != 30 {
		panic("node not filled completely")
	}
	return in, keys
}

func TestInternalKeysStaySingle(t *testing.T) {
	in, keys := MakeFilledNode()
	for _, k := range keys {
		if !slices.ContainsFunc(in.keys, func(key Bytes) bool {
			return bytes.Compare(key, k) == 0
		}) {
			t.Errorf("keys not filled correctly")
		}
	}
}

func TestInternalSortedKeys(t *testing.T) {
	in := MakeInternalNode(30)
	keys := MakeKeysWithGaps(20, 5)
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for i, key := range keys {
		_, newNode := in.setOrInsert(key, i*10+1)
		if newNode != nil {
			panic(fmt.Sprintf("possible test misconfiguration: at iter %d, more keys than can fit in single node", i))
		}
	}

	if !slices.IsSortedFunc(in.keys, func(a, b Bytes) int {
		return slices.Compare(a[:], b[:])
	}) {
		panic(fmt.Sprintf("test misconfiguration: keys are not sorted: %v", keys))
	}
}

func TestInternalCorrectValues(t *testing.T) {
	in, keys := MakeFilledNode()
	for i, key := range keys {
		r := in.valueRef(key)
		if r == nil {
			t.Errorf("prefilled key %v is nil", key)
		}

		if r != nil && *r != i*10+1 {
			t.Error("wrong value")
		}
	}

	// All the prefilled keys have only their first bytes filled, any Bytes with non-zero 2nd place will be new
	h := sha256.Sum256([]byte("Test bytes"))
	if r := in.valueRef(h[:]); r != nil {
		t.Error("value for unadded key")
	}
}

func TestInternalSplit(t *testing.T) {
	in, _ := MakeFilledNode()
	keys := make([]Bytes, 29, 30)
	copy(keys, in.keys)

	// 30 is the number of pointers here, so num keys in left split will be 14 and 15 in right
	// The key that would have been 15th is sent up
	testKey := Bytes{21}
	if _, found := sort.Find(len(keys), func(i int) int {
		return slices.Compare(testKey[:], keys[i][:])
	}); found == true {
		panic("test misconfiguration: testKey already exists")
	}

	keys = append(keys, testKey)
	slices.SortFunc(keys, func(a, b Bytes) int {
		return bytes.Compare(a, b)
	})

	upKey, newNode := in.setOrInsert(testKey, rand.Int())
	node := newNode.(*InternalNode[int])

	lkn := len(in.keys)
	rkn := len(node.keys)
	if abs(lkn-rkn) > 1 && lkn+rkn != 29 {
		t.Errorf("wrong number of keys: %d and %d", lkn, rkn)
	}

	expectedUpKey := keys[lkn]
	if !bytes.Equal(upKey, expectedUpKey) {
		t.Errorf("wrong value: %d Expected: %d", upKey, expectedUpKey)
	}

	lpn := len(in.pointers)
	rpn := len(node.pointers)
	if lpn != lkn+1 && rpn != rkn+1 {
		t.Errorf("wrong number of pointers: %d and %d", lpn, rpn)
	}

	// check that all keys exist
	retr_keys := make([]Bytes, 0, 30)
	retr_keys = append(retr_keys, in.keys...)
	retr_keys = append(retr_keys, upKey)
	retr_keys = append(retr_keys, node.keys...)

	if slices.CompareFunc(keys, retr_keys, func(b1, b2 Bytes) int {
		r := bytes.Compare(b1, b2)
		return r
	}) != 0 {
		t.Errorf("missing/duplicated keys")
	}
}

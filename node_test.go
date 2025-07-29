package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
)

func FillNode(node Node, maxInserts int, valueBuf Bytes) (int, []Hash, []int) {
	keys, values := GetData(maxInserts)
	numInserts := 0

	// inserts all elements at the front
	for i := 0; i < maxInserts; i++ {
		binary.BigEndian.PutUint64(valueBuf, uint64(values[i]))
		inserted := node.InsertAt(keys[i][:], valueBuf, 0)
		if !inserted {
			break
		}
		numInserts++
	}

	retKeys := keys[:numInserts]
	retValues := values[:numInserts]
	slices.Reverse(retKeys)
	slices.Reverse(retValues)
	return numInserts, retKeys, retValues
}

func AssertAllKVs(node Node, keys []Bytes, values []int, del bool) {
	nk := node.NumKeys()
	assertErr(nk == len(keys) && len(keys) == len(values), "invalid args for test")
	for i := range nk {
		AssertCell(node.KeyPtrCellForIndex(i), node.Heap, keys[i][:], values[i], del)
	}
}

func AssertAllKVsHash(node Node, keys []Hash, values []int, del bool) {
	sl := make([]Bytes, len(keys))
	for i := range keys {
		sl[i] = keys[i][:]
	}

	AssertAllKVs(node, sl, values, del)
}

func AssertCell(kp KeyPtrCell, heap Heap, key Bytes, value int, del bool) {
	assertErr(kp.Length() == len(key), "length mismatch")
	assertErr(bytes.Equal(kp.Head(), key[:PtrHeadSize]), "head mismatch")
	assertErr(kp.IsDeleted() == del, "delete mismatch")

	if del {
		return
	}
	vc := kp.ValueCellIn(heap)
	assertErr(bytes.Equal(vc.Key(), key), "key mismatch")
	assertErr(binary.BigEndian.Uint64(vc.Value()) == uint64(value), "value mismatch")
}

func TestNodeAlloc(t *testing.T) {
	node := AllocNode(InternalNodeTyp)

	assertErr(node.Type() == InternalNodeTyp, "node type mismatch")
	assertErr(node.Occupied() == 0, "node should be empty")
	assertErr(node.NumKeys() == 0, "node should have no keys")
	assertErr(node.ValuesOffset() == len(node.Heap), "values offset should be at the end of the heap")
	assertErr(node.ExtraPointerId() == NullPageId, "extra pointer id should be null")
	assertErr(len(node.Header) == sm.PageSize(), "node header size mismatch")
	assertErr(node.HeapSize() == sm.PageSize()-HeaderSize, "heap size mismatch")

	assertErr(node.Occupancy() == 0, "node occupancy should be 0")
	assertErr(node.Fragmentation() == 255, "node fragmentation should be 255 when empty")
}

func TestNodeInsert(t *testing.T) {
	node := AllocNode(0xff) // Using a dummy type for testing

	maxInserts := 100

	intBuff := make([]byte, 8)
	numInserts, keys, values := FillNode(node, maxInserts, intBuff)
	valCellSize := CalcValueCellSize(keys[0][:], intBuff)
	expSpaceTaken := numInserts * (KeyCellSize + valCellSize)
	expValuesOff := node.HeapSize() - numInserts*valCellSize

	assertErr(node.NumKeys() == numInserts, "number of keys mismatch after insertions")
	assertErr(node.Occupied() == expSpaceTaken, "node should have occupied the expected space after insertions")
	assertErr(node.Occupancy() == byte(expSpaceTaken*255/node.HeapSize()), "node occupancy should be equal to the expected")
	assertErr(node.ValuesOffset() == expValuesOff, "values offset should match expected value after insertions")
	assertErr(node.Fragmentation() == 255, "node fragmentation should be 255 in a node without deletions")
	assertErr(node.FreeSpace() == node.HeapSize()-expSpaceTaken, "node free space should be equal to the expected")
	assertErr(node.UnfragmentedFreeSpace() == node.FreeSpace(), "node unfragmented free space should be equal to the regular free space without deletions")

	for i := 0; i < numInserts; i++ {
		//index := numInserts - i - 1
		assertErr(node.sizeForKey(i) == expSpaceTaken/numInserts, "size for each key-value pair in node should be equal to the expected")

		kp := node.KeyPtrCellForIndex(i)
		AssertCell(kp, node.Heap, keys[i][:], values[i], false)
	}
}

func TestNodeDeleteCompact(t *testing.T) {
	node := AllocNode(0xff)
	maxInserts := 100
	delProb := 0.2

	intBuff := make([]byte, 8)
	nkeys, keys, values := FillNode(node, maxInserts, intBuff)
	entrySize := node.sizeForKey(0)

	assertErr(node.Occupied() == nkeys*entrySize, "Occupied should be equal to the expected")
	assertErr(node.Fragmentation() == 255, "node fragmentation should be 255 in a node without deletions")

	deleted := NewSet[int]()
	for i := range keys {
		del := rand.Uint32() < uint32(math.MaxUint32*delProb)
		if del {
			deleted.Add(i)
			node.DeleteAtIndex(i)
		}
	}
	sz := deleted.Len()
	fmt.Println(sz)
	for i := range deleted.All() {
		assertErr(node.KeyPtrCellForIndex(i).IsDeleted(), "deleted key-value pair in node should be deleted")
	}

	for i := range node.NumKeys() {
		kp := node.KeyPtrCellForIndex(i)
		del := kp.IsDeleted()
		if deleted.Contains(i) {
			assertErr(del, "deleted key-value pair in node should be deleted")
		} else {
			AssertCell(kp, node.Heap, keys[i][:], values[i], false)
		}
	}

	assertErr(node.Occupied() == (nkeys-deleted.Len())*entrySize+deleted.Len()*KeyCellSize, "Occupied should be equal to the expected")
	assertErr(node.Fragmentation() < 255, "node fragmentation should be less than 255")

	nonDelKeyList := make([]Bytes, 0)
	nonDelValList := make([]int, 0)
	for i, key := range keys {
		if !deleted.Contains(i) {
			nonDelKeyList = append(nonDelKeyList, key[:])
			nonDelValList = append(nonDelValList, values[i])
		}
	}

	// compact
	preFree := node.FreeSpace()
	nf := node.Compact()
	postFree := node.FreeSpace()

	assertErr(node.NumKeys() == nkeys-sz, "number of keys mismatch after compaction")
	assertErr(node.NumKeys() == len(nonDelKeyList), "number of non del keys mismatch after compaction")

	// verify that all non-deleted keys are present after compaction
	AssertAllKVs(node, nonDelKeyList, nonDelValList, false)
	assertErr(preFree+nf == postFree, "preFree + freed space and postFree should be equal")
	assertErr(node.Fragmentation() == 255, "node fragmentation should be 255 in a compacted node but is", node.Fragmentation())
}

func TestNodeSplit(t *testing.T) {
	node := AllocNode(0xff)
	maxInserts := 100

	intBuff := make([]byte, 8)
	nkeys, keys, values := FillNode(node, maxInserts, intBuff)
	assert(nkeys < maxInserts, "node not fully filled, increase `maxInserts`")

	assertErr(node.NumKeys() == len(keys), "number of keys mismatch before expected failed insert")

	ins := node.InsertAt(keys[0][:], intBuff, node.NumKeys())
	assertErr(!ins, "expected insertion to fail, but succeeded")
	assertErr(node.NumKeys() == len(keys), "number of keys mismatch after expected failed insert")
	AssertAllKVsHash(node, keys, values, false)

	halfK := node.HalfSizeK()
	spl := node.Split()
	assertErr(node.NumKeys() == halfK, "number of keys mismatch in left node after split")
	assertErr(spl.NumKeys() == nkeys-halfK, "number of keys mismatch in right node after split")
}

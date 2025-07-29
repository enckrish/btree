package btree

import (
	"bytes"
	"fmt"
	"sort"
)

type Node struct {
	// Header should point to the whole page, not a slice of it
	Header
	Heap
}

func AsNode(data Bytes) Node {
	assertErr(len(data) == sm.PageSize(), ErrImplementationError.Error())
	return Node{data, data[HeaderSize:]}
}

func AllocNode(typ byte) Node {
	id, data, err := sm.Alloc()
	if err != nil {
		panic(err)
	}

	node := AsNode(data)
	node.SetId(id)
	node.SetType(typ)
	node.SetOccupied(0)
	node.SetExtraPointerId(NullPageId)
	node.SetValuesOffset(Offset(len(node.Heap)))
	node.SetNumKeys(0)

	return node
}

func (n Node) IsNil() bool {
	return len(n.Header) == 0
}

func (n Node) ValidIndex(index int) bool {
	return index >= 0 && index < n.NumKeys()
}

// HeapSize Note: should be equal to page_size - HeaderSize as well
func (n Node) HeapSize() int {
	return len(n.Header) - HeaderSize
}

// Occupancy is a 255-scaled value, where 255 means the heap is fully occupied and 0 means it is empty.
func (n Node) Occupancy() byte {
	return byte(int(n.Occupied()) * 255 / n.HeapSize())
}

// Fragmentation returns how much times more space is the heap taking than required.
// Since Occupied() must always be <= actual space occupied, Fragmentation lies between 255 and 0, 255 being the best.
func (n Node) Fragmentation() byte {
	actualSpaceNeeded := n.NumKeys()*KeyCellSize + n.HeapSize() - n.ValuesOffset()
	occp := n.Occupied()
	if actualSpaceNeeded == 0 {
		if occp == 0 {
			return 255
		} else {
			return 0 // At this point, Compact() should be called to reset values offset
		}
	}
	return byte(occp * 255 / actualSpaceNeeded)
}

// TODO verify that it works well on all node types
func (n Node) KeyPtrCellForIndex(i int) KeyPtrCell {
	if !n.ValidIndex(i) {
		return nil
	}
	return KeyPtrCell(n.Heap[KeyCellSize*i:])
}

func (n Node) ValueCellForIndex(i int) ValueCell {
	if !n.ValidIndex(i) {
		return nil
	}
	kc := n.KeyPtrCellForIndex(i)

	if kc.IsDeleted() {
		return nil
	}
	vOff := kc.At()
	return ValueCell(n.Heap[vOff:])
}

// appendValueCell must be called after checking if free space is available, returns the offset at which the cell was appended
func (n Node) appendValueCell(cell ValueCell) int {
	at := n.ValuesOffset() - cell.Size()
	n.SetValuesOffset(Offset(at))
	copy(n.Heap[at:at+cell.Size()], cell)
	return at
}

// PurgeDeletedKeys compacts the pointer table by removing deleted key entries
func (n Node) PurgeDeletedKeys() {
	delKeys := 0
	srcPos := 0
	dstPos := 0
	for range n.NumKeys() {
		if KeyPtrCell(n.Heap[srcPos:]).IsDeleted() {
			delKeys++
		} else {
			if srcPos != dstPos {
				copy(n.Heap[dstPos:], n.Heap[srcPos:srcPos+KeyCellSize])
			}
			dstPos += KeyCellSize
		}

		srcPos += KeyCellSize
	}

	n.SetNumKeys(uint16(n.NumKeys() - delKeys))
	n.DecOccupied(delKeys * KeyCellSize)
}

// LowerBoundForKey returns the least index of any stored key that is >= key
// its value is in [1, n], where n = node.NumKeys(), n when key is greater than all stored keys
// The least value is 1 and not zero because we only split a node from the right.
// Only works on sorted lists
func (n Node) LowerBoundForKey(key Bytes) int {
	// TODO use heads for faster upper bound calculation
	n.PurgeDeletedKeys() // TODO shouldn't need this in final version
	i := sort.Search(n.NumKeys(), func(i int) bool {
		kv := n.ValueCellForIndex(i)
		return bytes.Compare(key, kv.Key()) <= 0
	})
	return i
}

func (n Node) KeyExists(key Bytes) bool {
	// Note: key deletes are handles unideally in the lower-bound function
	// ideally we would like it to have no side effects (a non-state changing function)
	idx := n.LowerBoundForKey(key)
	if idx == n.NumKeys() {
		return false
	}

	if bytes.Equal(n.KeyPtrCellForIndex(idx).ValueCellIn(n.Heap).Key(), key) {
		return true
	}

	return false
}

func (n Node) FreeSpace() int {
	return n.ValuesOffset() - (n.NumKeys() * KeyCellSize)
}

func (n Node) UnfragmentedFreeSpace() int {
	size := 0
	for i := range n.NumKeys() {
		if !n.KeyPtrCellForIndex(i).IsDeleted() {
			size += n.sizeForKey(i)
		}
	}
	return n.HeapSize() - size
}

// InsertAt inserts at the given pos; pos is wrt to keys and not pointer, which means the first pointer in internal and
// last sibling pointer in leaf aren't covered by this method.
// Returns false if the node is full and cannot accommodate the new key-value pair.
// WARNING: arbitrary use may make node unsorted.
// Insertion with splitting is implemented by the specific nodes using this method.
func (n Node) InsertAt(key Bytes, value Bytes, index int) bool {
	assertErr(index <= n.NumKeys(), ErrImplementationError.Error())

	// check if space is available
	// TODO current split mechanism dictates that maximum new entry size is less than HeapSize//2
	valueSize := CalcValueCellSize(key, value)
	reqSpace := KeyCellSize + valueSize
	if reqSpace > n.FreeSpace() {
		if reqSpace > n.UnfragmentedFreeSpace() {
			return false
		}
		n.Compact()
	}

	// shift existing keys to make space for the new key
	ptrInsertOffset := index * KeyCellSize
	copy(n.Heap[ptrInsertOffset+KeyCellSize:], n.Heap[ptrInsertOffset:n.NumKeys()*KeyCellSize])

	valueOffset := n.ValuesOffset() - valueSize
	KeyPtrCell(n.Heap[ptrInsertOffset:]).Set(key, Offset(valueOffset))
	ValueCell(n.Heap[valueOffset:]).Set(0x0, key, value)

	n.SetNumKeys(uint16(n.NumKeys() + 1))
	n.SetValuesOffset(Offset(valueOffset))
	n.IncOccupied(reqSpace)
	return true
}

func (n Node) DeleteAtIndex(i int) bool {
	if !n.ValidIndex(i) {
		return false
	}

	kp := n.KeyPtrCellForIndex(i)
	valSize := kp.ValueCellIn(n.Heap).Size()
	kp.Delete()

	n.DecOccupied(valSize)
	return true
}

func (n Node) sizeForKey(i int) int {
	assertErr(n.ValidIndex(i), ErrImplementationError.Error())
	c := n.ValueCellForIndex(i)
	csize := 0
	if c != nil {
		csize = c.Size()
	}
	return KeyCellSize + csize
}

// HalfSizeK returns the number of nodes from end to remove to approximately halve the heap size
func (n Node) HalfSizeK() int {
	mostSize := len(n.Heap) / 2
	size := 0
	i := 0
	for i = range n.NumKeys() {
		size += n.sizeForKey(i)
		if size >= mostSize {
			break
		}
	}

	return i
}

// MoveKeysFrom moves end keys-values from `off` to the start of `to` node
func (n Node) MoveKeysFrom(to Node, off Offset) {
	totalSize := 0
	numKeys := 0
	for off < Offset(n.NumKeys()*KeyCellSize) {
		totalSize += KeyPtrCell(n.Heap[off:]).ValueCellIn(n.Heap).Size() + KeyCellSize
		numKeys++
		off += KeyCellSize
	}

	if totalSize > to.FreeSpace() {
		if totalSize <= to.UnfragmentedFreeSpace() {
			to.Compact()
		} else {
			panic(ErrImplementationError)
		}
	}

	// make way for k KeyPtrCells at the start of to
	copy(to.Heap[numKeys*KeyCellSize:], to.Heap[:to.NumKeys()*KeyCellSize])
	// copy KeyPtrCells into `to`
	copy(to.Heap[:numKeys*KeyCellSize], n.Heap[off:n.NumKeys()*KeyCellSize])

	for off < Offset(n.NumKeys()*KeyCellSize) {
		kp := KeyPtrCell(n.Heap[off:])
		vc := kp.ValueCellIn(n.Heap)
		to.appendValueCell(vc)

		kp.Delete()
	}

	n.SetNumKeys(uint16(n.NumKeys() - numKeys))
	n.DecOccupied(totalSize)

	to.SetNumKeys(uint16(to.NumKeys() + numKeys))
	n.IncOccupied(totalSize)
}

func (n Node) Split() Node {
	halfK := n.HalfSizeK()
	ptrOff := halfK * KeyCellSize

	spl := AllocNode(n.Type())
	n.MoveKeysFrom(spl, Offset(ptrOff))

	return spl
}

func (n Node) CompactValueCells() {
	// get offsets for all the value cells which have a non deleted pointer table
	vOffs := make([]int, 0, n.NumKeys())
	offMap := make(map[int]int)
	for i := range n.NumKeys() {
		if c := n.KeyPtrCellForIndex(i); !c.IsDeleted() {
			off := c.At()
			vOffs = append(vOffs, off)
			offMap[off] = i
		}
	}

	sort.Sort(sort.Reverse(sort.IntSlice(vOffs)))

	moveOffset := len(n.Heap)
	for _, off := range vOffs {
		sz := ValueCell(n.Heap[off:]).Size()
		moveOffset -= sz
		copy(n.Heap[moveOffset:], n.Heap[off:off+sz])
		n.KeyPtrCellForIndex(offMap[off]).SetAt(Offset(moveOffset))
	}

	n.SetValuesOffset(Offset(moveOffset))
}

// Compact returns freed space
func (n Node) Compact() int {
	if n.Occupied() == 0 {
		n.SetValuesOffset(Offset(len(n.Heap)))
	}
	preSpace := n.FreeSpace()
	preUnfragSpace := n.UnfragmentedFreeSpace()

	n.CompactValueCells()
	n.PurgeDeletedKeys()

	postSpace := n.FreeSpace()
	fmt.Println(preSpace, postSpace, preUnfragSpace, n.NumKeys())
	assertErr(postSpace == preUnfragSpace, ErrImplementationError.Error())

	return postSpace - preSpace
}

type TraversalPositions struct {
	node InternalNode
	pos  int
}

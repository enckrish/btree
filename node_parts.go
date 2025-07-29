package btree

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
)

var ErrImplementationError = errors.New("implementation error")
var ErrUnimplemented = errors.New("unimplemented error")

// ALERT: Don't change these! In case of change, also compute and update
// new boundaries of any fields that use them or follow them.

type PageId = uint32
type Offset = uint16

const PtrHeadSize = 5
const NullPageId PageId = math.MaxUint32

// TODO move to config file
const PageSize = 4096

const LeafNodeTyp = 0x01
const InternalNodeTyp = 0x02

// Header contents:
// id PageId: page id of self
// typ byte: type of data in the page
// occupied uint16: total space occupied in heap
// extraPtr PageId: extra pointer in internal nodes and next pointer in leaf nodes
// valuesOffset Offset: offset at which new values are to be put, keys offset can be easily calculated using nkeys, so not stored.
// nkeys uint16: number of keys stored
type Header Bytes

const HeaderSize = 4 + 1 + 1 + 1 + 4 + 2 + 2

func (h Header) Id() PageId {
	return binary.LittleEndian.Uint32(h)
}

func (h Header) SetId(id PageId) {
	binary.LittleEndian.PutUint32(h, id)
}

func (h Header) Type() byte {
	return h[4]
}

func (h Header) SetType(t byte) {
	h[4] = t
}

// Occupied returns the actual total space occupied in the heap
func (h Header) Occupied() int {
	return int(binary.BigEndian.Uint16(h[5:]))
}

func (h Header) IncOccupied(add int) {
	binary.BigEndian.PutUint16(h[5:], uint16(h.Occupied()+add))
}

func (h Header) DecOccupied(del int) {
	binary.BigEndian.PutUint16(h[5:], uint16(h.Occupied()-del))
}

func (h Header) SetOccupied(o uint16) {
	binary.BigEndian.PutUint16(h[5:], o)
}

func (h Header) ExtraPointerId() PageId {
	return binary.BigEndian.Uint32(h[7:])
}

func (h Header) SetExtraPointerId(id PageId) {
	binary.BigEndian.PutUint32(h[7:], id)
}

func (h Header) ValuesOffset() int {
	return int(binary.BigEndian.Uint16(h[11:]))
}

func (h Header) SetValuesOffset(v Offset) {
	binary.BigEndian.PutUint16(h[11:], v)
}

func (h Header) NumKeys() int {
	return int(binary.BigEndian.Uint16(h[13:]))
}

func (h Header) SetNumKeys(n uint16) {
	binary.BigEndian.PutUint16(h[13:], n)
}

func (h Header) String() string {
	s := fmt.Sprintf("Id: %d\nType: %d\nOccupied: %d/%d\nExtraPointerId: %d\nValuesOffset: %d\nNumKeys: %d",
		h.Id(), h.Type(), h.Occupied(), PageSize, h.ExtraPointerId(),
		h.ValuesOffset(), h.NumKeys())
	return s
}

type Heap Bytes

// KeyPtrCell contents:
// length uint16: length of the key
// head [5]byte: a copy of the beginning of the key for faster search
// at Offset: offset at which it's ValueCell exists
type KeyPtrCell Bytes

const KeyCellSize = 2 + PtrHeadSize + 2

func (kc KeyPtrCell) Set(key Bytes, valueOffset Offset) {
	kc.SetLength(uint16(len(key)))
	// This is to support keys < PtrHeadSize in length
	// we aren't directly writing to the heap because it may contain garbage values
	// but we only want 0s in empty positions.
	buf := make(Bytes, PtrHeadSize)
	copy(buf, key)
	kc.SetHead(key)
	kc.SetAt(valueOffset)
}

func (kc KeyPtrCell) Length() int {
	return int(binary.BigEndian.Uint16(kc))
}

func (kc KeyPtrCell) SetLength(l uint16) {
	binary.BigEndian.PutUint16(kc, l)
}

func (kc KeyPtrCell) Head() Bytes {
	return kc[2 : 2+PtrHeadSize]
}

func (kc KeyPtrCell) SetHead(key Bytes) {
	copy(kc[2:2+PtrHeadSize], key)
}

func (kc KeyPtrCell) At() int {
	return int(binary.BigEndian.Uint16(kc[2+PtrHeadSize:]))
}

func (kc KeyPtrCell) SetAt(at Offset) {
	binary.BigEndian.PutUint16(kc[2+PtrHeadSize:], at)
}

func (kc KeyPtrCell) IsDeleted() bool {
	return kc.At() == math.MaxUint16
}

func (kc KeyPtrCell) Delete() {
	kc.SetAt(math.MaxUint16)
}

func (kc KeyPtrCell) ValueCellIn(heap Bytes) ValueCell {
	return heap[kc.At():]
}

// ValueCell contains:
// flags byte: a byte of various flags
// keyLen uint16: length of key
// valueLen uint16: length of value
// key Bytes: the key itself
// value Bytes: the value itself
type ValueCell Bytes

func CalcValueCellSize(key, val Bytes) int {
	return 1 + 2 + 2 + len(key) + len(val)
}

func (kv ValueCell) Size() int {
	return 1 + 2 + 2 + kv.KeyLen() + kv.ValueLen()
}

func (kv ValueCell) Set(flags byte, key Bytes, val Bytes) {
	kv.SetFlags(flags)
	kv.setKey(key)
	kv.setValue(val)
}

// setKey operation may affect this and consecutive cells, therefore not meant for use publicly
func (kv ValueCell) setKey(k Bytes) {
	kv.SetKeyLen(uint16(len(k)))
	copy(kv[5:], k)
}

// setValue operation may affect this and consecutive cells, therefore not meant for use publicly
// only to be used after setKey has been called
func (kv ValueCell) setValue(val Bytes) {
	kv.SetValueLen(uint16(len(val)))
	copy(kv[5+kv.KeyLen():], val)
}

func (kv ValueCell) Flags() byte {
	return kv[0]
}

func (kv ValueCell) SetFlags(f byte) {
	kv[0] = f
}

func (kv ValueCell) KeyLen() int {
	return int(binary.BigEndian.Uint16(kv[1:3]))
}

func (kv ValueCell) SetKeyLen(k uint16) {
	binary.BigEndian.PutUint16(kv[1:], k)
}

func (kv ValueCell) ValueLen() int {
	return int(binary.BigEndian.Uint16(kv[3:5]))
}

func (kv ValueCell) SetValueLen(v uint16) {
	binary.BigEndian.PutUint16(kv[3:], v)
}

func (kv ValueCell) Key() Bytes {
	return kv[5 : 5+kv.KeyLen()]
}

func (kv ValueCell) Value() Bytes {
	return kv[5+kv.KeyLen() : 5+kv.KeyLen()+kv.ValueLen()]
}

func (kv ValueCell) String() string {
	key := hex.EncodeToString(kv.Key())
	val := hex.EncodeToString(kv.Value())
	return fmt.Sprintf("[%s, %s]", key, val)
}

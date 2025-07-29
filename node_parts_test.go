package btree

import (
	// "fmt"

	"bytes"
	"math/rand"
	"testing"
)

// Header

func TestHeaderSetGet(t *testing.T) {
	h := Header(make(Bytes, PageSize))

	id := PageId(42) // rand.Uint32()
	typ := byte(rand.Uint32())
	occp := 42
	extraPtr := rand.Uint32()
	valOff := Offset(rand.Uint32())
	nkeys := uint16(10)

	h.SetId(id)
	h.SetType(typ)
	h.SetOccupied(Offset(occp))
	h.SetExtraPointerId(extraPtr)
	h.SetValuesOffset(valOff)
	h.SetNumKeys(nkeys)

	assertErr(h.Id() == id, "id mismatch")
	assertErr(h.Type() == typ, "type mismatch")
	assertErr(h.Occupied() == occp, "offset mismatch")
	assertErr(h.ExtraPointerId() == extraPtr, "extraptr mismatch")
	assertErr(h.ValuesOffset() == int(valOff), "valoff mismatch")
	assertErr(h.NumKeys() == int(nkeys), "nkeys mimatch")

	occChg := 67
	h.IncOccupied(occChg)
	assertErr(h.Occupied() == occp+occChg, "occp inc issue")
	h.DecOccupied(occChg)
	assertErr(h.Occupied() == occp, "occp dec issue")
}

// KeyPtrCell

func TestKPtrCellSetGet(t *testing.T) {
	kp := KeyPtrCell(make(Bytes, PageSize))

	key := RandASCIIByte32(0)
	valOff := Offset(42)

	kp.Set(key[:], valOff)

	assertErr(!kp.IsDeleted(), "deleted mismatch in new")
	assertErr(kp.Length() == len(key), "length mismatch")
	assertErr(bytes.Equal(kp.Head(), key[:PtrHeadSize]), "head mismatch")
	assertErr(kp.At() == int(valOff), "at mismatch")

	kp.Delete()
	assertErr(kp.IsDeleted(), "deleted mismatch after deleted")
}

// ValueCell

func TestValueCellSetGet(t *testing.T) {
	vc := ValueCell(make(Bytes, PageSize))

	key := RandASCIIByte32(0)
	val := RandASCIIByte32(0)
	flags := byte(rand.Uint32())

	vc.Set(flags, key[:], val[:])

	assertErr(vc.Flags() == flags, "flags mismatch")
	assertErr(bytes.Equal(vc.Key(), key[:]), "key mismatch")
	assertErr(bytes.Equal(vc.Value(), val[:]), "value mismatch")
	assertErr(vc.KeyLen() == len(key), "key length mismatch")
	assertErr(vc.ValueLen() == len(val), "value length mismatch")
	assertErr(vc.Size() == CalcValueCellSize(key[:], val[:]), "size mismatch")
}

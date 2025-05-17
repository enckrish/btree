package btree

type Map[K any, V any] struct {
	*BTree[V]
	hashFn func(K) Hash
}

func NewMap[K any, V any](degree int, hashFn func(K) Hash, expectedHeight int) *Map[K, V] {
	btree := NewBTree[V](degree, expectedHeight)
	return &Map[K, V]{
		BTree:  btree,
		hashFn: hashFn,
	}
}

func (m Map[K, V]) Set(key K, v *V) {
	h := m.hashFn(key)
	m.SetOp(h[:], v)
}

func (m Map[K, V]) Get(key K) *V {
	h := m.hashFn(key)
	v := m.GetOp(h[:])
	return v
}

func (m Map[K, V]) Del(key K) bool {
	h := m.hashFn(key)
	return m.DelOp(h[:], false)
}

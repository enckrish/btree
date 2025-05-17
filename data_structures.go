package btree

type Set[T comparable] map[T]struct{}

func NewSet[T comparable]() Set[T] {
	m := make(Set[T])
	return m
}

func (s Set[T]) Add(e T) (dup bool) {
	if s.Contains(e) {
		return true
	}

	s[e] = struct{}{}
	return false
}

func (s Set[T]) Remove(e T) {
	delete(s, e)
}

func (s Set[T]) Contains(e T) bool {
	_, ok := s[e]
	return ok
}

func (s Set[T]) Len() int {
	return len(s)
}

type Stack[T any] []T

func NewStack[T any](cap int) Stack[T] {
	return make(Stack[T], 0, cap)
}

func (s *Stack[T]) Push(a T) {
	*s = append(*s, a)
}

func (s *Stack[T]) Pop() (T, bool) {
	if len(*s) == 0 {
		var zero T
		return zero, false
	}
	v := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return v, true
}

func (s *Stack[T]) Empty() bool {
	return len(*s) == 0
}

func (s *Stack[T]) Top() *T {
	if len(*s) == 0 {
		return nil
	}
	v := &(*s)[len(*s)-1]
	return v
}

func (s *Stack[T]) Clear() {
	*s = (*s)[:0]
}

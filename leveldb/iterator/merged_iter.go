// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package iterator

import (
	"container/heap"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type dir int

const (
	dirReleased dir = iota - 1
	dirSOI
	dirEOI
	dirBackward
	dirForward
)

// 该迭代器可以把多个sst中的kv，整体按序迭代出来
type mergedIterator struct {
	cmp    comparer.Comparer
	iters  []Iterator // 存每个sst的indexedIterator
	strict bool

	keys     [][]byte // 存iters中每个iter(sst)中当前的min key，在迭代过程中Next时会更新
	index    int      // keys中当前最小key对应的iters索引，从而去iters中找到对应的iter，然后就可以拿到这个value了。外面调用Next时更新，mergedIterator本身实现了heap的功能
	dir      dir
	err      error
	errf     func(err error)
	releaser util.Releaser

	indexes []int // the heap of iterator indexes
	reverse bool  //nolint: structcheck // if true, indexes is a max-heap
}

func assertKey(key []byte) []byte {
	if key == nil {
		panic("leveldb/iterator: nil key")
	}
	return key
}

func (i *mergedIterator) iterErr(iter Iterator) bool {
	if err := iter.Error(); err != nil {
		if i.errf != nil {
			i.errf(err)
		}
		if i.strict || !errors.IsCorrupted(err) {
			i.err = err
			return true
		}
	}
	return false
}

func (i *mergedIterator) Valid() bool {
	return i.err == nil && i.dir > dirEOI
}

func (i *mergedIterator) First() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	// 用每个sst中的第一个key构建小顶堆
	h := i.indexHeap()
	h.Reset(false)
	for x, iter := range i.iters {
		switch {
		case iter.First(): // 每个sst中第一个kv
			i.keys[x] = assertKey(iter.Key())
			h.Push(x)
		case i.iterErr(iter):
			return false
		default:
			i.keys[x] = nil
		}
	}
	heap.Init(h)
	i.dir = dirSOI
	return i.next()
}

func (i *mergedIterator) Last() bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	h := i.indexHeap()
	h.Reset(true)
	for x, iter := range i.iters {
		switch {
		case iter.Last():
			i.keys[x] = assertKey(iter.Key())
			h.Push(x)
		case i.iterErr(iter):
			return false
		default:
			i.keys[x] = nil
		}
	}
	heap.Init(h)
	i.dir = dirEOI
	return i.prev()
}

func (i *mergedIterator) Seek(key []byte) bool {
	if i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	h := i.indexHeap()
	h.Reset(false)
	for x, iter := range i.iters {
		switch {
		case iter.Seek(key):
			i.keys[x] = assertKey(iter.Key())
			h.Push(x)
		case i.iterErr(iter):
			return false
		default:
			i.keys[x] = nil
		}
	}
	heap.Init(h)
	i.dir = dirSOI
	return i.next()
}

func (i *mergedIterator) next() bool {
	h := i.indexHeap()
	if h.Len() == 0 {
		i.dir = dirEOI
		return false
	}
	i.index = heap.Pop(h).(int)
	i.dir = dirForward
	return true
}

func (i *mergedIterator) Next() bool {
	if i.dir == dirEOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	switch i.dir {
	case dirSOI:
		return i.First()
	case dirBackward:
		key := append([]byte(nil), i.keys[i.index]...)
		if !i.Seek(key) {
			return false
		}
		return i.Next()
	}

	// 当前最小key的index，和对应的blockIter
	x := i.index
	iter := i.iters[x]
	switch {
	case iter.Next():
		i.keys[x] = assertKey(iter.Key())
		heap.Push(i.indexHeap(), x)
	case i.iterErr(iter):
		return false
	default:
		i.keys[x] = nil
	}
	return i.next()
}

func (i *mergedIterator) prev() bool {
	h := i.indexHeap()
	if h.Len() == 0 {
		i.dir = dirSOI
		return false
	}
	i.index = heap.Pop(h).(int)
	i.dir = dirBackward
	return true
}

func (i *mergedIterator) Prev() bool {
	if i.dir == dirSOI || i.err != nil {
		return false
	} else if i.dir == dirReleased {
		i.err = ErrIterReleased
		return false
	}

	switch i.dir {
	case dirEOI:
		return i.Last()
	case dirForward:
		key := append([]byte(nil), i.keys[i.index]...)
		h := i.indexHeap()
		h.Reset(true)
		for x, iter := range i.iters {
			if x == i.index {
				continue
			}
			seek := iter.Seek(key)
			switch {
			case seek && iter.Prev(), !seek && iter.Last():
				i.keys[x] = assertKey(iter.Key())
				h.Push(x)
			case i.iterErr(iter):
				return false
			default:
				i.keys[x] = nil
			}
		}
		heap.Init(h)
	}

	x := i.index
	iter := i.iters[x]
	switch {
	case iter.Prev():
		i.keys[x] = assertKey(iter.Key())
		heap.Push(i.indexHeap(), x)
	case i.iterErr(iter):
		return false
	default:
		i.keys[x] = nil
	}
	return i.prev()
}

func (i *mergedIterator) Key() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.keys[i.index]
}

func (i *mergedIterator) Value() []byte {
	if i.err != nil || i.dir <= dirEOI {
		return nil
	}
	return i.iters[i.index].Value()
}

func (i *mergedIterator) Release() {
	if i.dir != dirReleased {
		i.dir = dirReleased
		for _, iter := range i.iters {
			iter.Release()
		}
		i.iters = nil
		i.keys = nil
		i.indexes = nil
		if i.releaser != nil {
			i.releaser.Release()
			i.releaser = nil
		}
	}
}

func (i *mergedIterator) SetReleaser(releaser util.Releaser) {
	if i.dir == dirReleased {
		panic(util.ErrReleased)
	}
	if i.releaser != nil && releaser != nil {
		panic(util.ErrHasReleaser)
	}
	i.releaser = releaser
}

func (i *mergedIterator) Error() error {
	return i.err
}

func (i *mergedIterator) SetErrorCallback(f func(err error)) {
	i.errf = f
}

func (i *mergedIterator) indexHeap() *indexHeap {
	return (*indexHeap)(i)
}

// NewMergedIterator returns an iterator that merges its input. Walking the
// resultant iterator will return all key/value pairs of all input iterators
// in strictly increasing key order, as defined by cmp.
// The input's key ranges may overlap, but there are assumed to be no duplicate
// keys: if iters[i] contains a key k then iters[j] will not contain that key k.
// None of the iters may be nil.
//
// If strict is true the any 'corruption errors' (i.e errors.IsCorrupted(err) == true)
// won't be ignored and will halt 'merged iterator', otherwise the iterator will
// continue to the next 'input iterator'.
func NewMergedIterator(iters []Iterator, cmp comparer.Comparer, strict bool) Iterator {
	return &mergedIterator{
		iters:   iters,
		cmp:     cmp,
		strict:  strict,
		keys:    make([][]byte, len(iters)),
		indexes: make([]int, 0, len(iters)),
	}
}

// indexHeap implements heap.Interface.
// 小顶堆
type indexHeap mergedIterator

func (h *indexHeap) Len() int { return len(h.indexes) }
func (h *indexHeap) Less(i, j int) bool {
	i, j = h.indexes[i], h.indexes[j]
	r := h.cmp.Compare(h.keys[i], h.keys[j])
	if h.reverse {
		return r > 0
	}
	return r < 0
}

func (h *indexHeap) Swap(i, j int) {
	h.indexes[i], h.indexes[j] = h.indexes[j], h.indexes[i]
}

func (h *indexHeap) Push(value interface{}) {
	h.indexes = append(h.indexes, value.(int))
}

func (h *indexHeap) Pop() interface{} {
	e := len(h.indexes) - 1
	popped := h.indexes[e]
	h.indexes = h.indexes[:e]
	return popped
}

func (h *indexHeap) Reset(reverse bool) {
	h.reverse = reverse
	h.indexes = h.indexes[:0]
}

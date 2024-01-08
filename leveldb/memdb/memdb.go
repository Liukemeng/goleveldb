// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Package memdb provides in-memory key/value database implementation.
package memdb

import (
	"math/rand"
	"sync"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Common errors.
var (
	ErrNotFound     = errors.ErrNotFound
	ErrIterReleased = errors.New("leveldb/memdb: iterator released")
)

const tMaxHeight = 12

type dbIter struct {
	util.BasicReleaser
	p          *DB
	slice      *util.Range
	node       int
	forward    bool // 表明是否已经开始向后迭代
	key, value []byte
	err        error
}

// 找到node对应的kv,补充进dbIter.key value
func (i *dbIter) fill(checkStart, checkLimit bool) bool {
	if i.node != 0 {
		n := i.p.nodeData[i.node]          // key 的起始地址， 0
		m := n + i.p.nodeData[i.node+nKey] // key 的终止地址， 1
		i.key = i.p.kvData[n:m]
		if i.slice != nil {
			switch {
			case checkLimit && i.slice.Limit != nil && i.p.cmp.Compare(i.key, i.slice.Limit) >= 0:
				fallthrough
			case checkStart && i.slice.Start != nil && i.p.cmp.Compare(i.key, i.slice.Start) < 0:
				i.node = 0
				goto bail
			}
		}
		i.value = i.p.kvData[m : m+i.p.nodeData[i.node+nVal]]
		return true
	}
bail:
	i.key = nil
	i.value = nil
	return false
}

func (i *dbIter) Valid() bool {
	return i.node != 0
}

func (i *dbIter) First() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil {
		i.node, _ = i.p.findGE(i.slice.Start, false)
	} else {
		i.node = i.p.nodeData[nNext]
	}
	return i.fill(false, true)
}

func (i *dbIter) Last() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Limit != nil {
		i.node = i.p.findLT(i.slice.Limit)
	} else {
		i.node = i.p.findLast()
	}
	return i.fill(true, false)
}

func (i *dbIter) Seek(key []byte) bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	if i.slice != nil && i.slice.Start != nil && i.p.cmp.Compare(key, i.slice.Start) < 0 {
		key = i.slice.Start
	}
	i.node, _ = i.p.findGE(key, false)
	return i.fill(false, true)
}

func (i *dbIter) Next() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if !i.forward {
			return i.First()
		}
		return false
	}
	i.forward = true
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.nodeData[i.node+nNext]
	return i.fill(false, true)
}

func (i *dbIter) Prev() bool {
	if i.Released() {
		i.err = ErrIterReleased
		return false
	}

	if i.node == 0 {
		if i.forward {
			return i.Last()
		}
		return false
	}
	i.forward = false
	i.p.mu.RLock()
	defer i.p.mu.RUnlock()
	i.node = i.p.findLT(i.key)
	return i.fill(true, false)
}

func (i *dbIter) Key() []byte {
	return i.key
}

func (i *dbIter) Value() []byte {
	return i.value
}

func (i *dbIter) Error() error { return i.err }

func (i *dbIter) Release() {
	if !i.Released() {
		i.p = nil
		i.node = 0
		i.key = nil
		i.value = nil
		i.BasicReleaser.Release()
	}
}

const (
	nKV = iota
	nKey
	nVal
	nHeight
	nNext
)

/*


k1-> 1 添加进  12层；
k3-> 3 添加进  9层；
k2-> 2 添加进  11层；

初始时：
kvData:
0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
nodeData:
/第一个node是header，index部分是0表示是尾节点
0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|0	|0	|0	|12	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
prevNode:
0	1	2	3	4	5	6	7	8	9	10	11
---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0
---	--- ---	--- ---	--- ---	--- ---	--- ---	---


一: put k1->1 to  12 levels
kvData:
0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|k	|1	|1	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
nodeData:
/第一个node是header
																/第二个node
0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19	20	21	22	23	24	25	26	27	28	29	30	31
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---	--- ---
|0	|0	|0	|12	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|0	|2	|1	|12	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---	--- ---
prevNode:
0	1	2	3	4	5	6	7	8	9	10	11
---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0
---	--- ---	--- ---	--- ---	--- ---	--- ---	---

二: put k3->3 to  9 levels
kvData:
0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|k	|1	|1	|k	|3	|3	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
nodeData:
/第一个node是header
																/第二个node k1
																																/第三个node k3
0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19	20	21	22	23	24	25	26	27	28	29	30	31	32	33	34	35	36	37	38	39	40	41	42	43	44
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|0	|0	|0	|12	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|0	|2	|1	|12	|32	|32	|32	|32	|32	|32	|32	|32	|32	|0	|0	|0	|6	|2	|1	|9	|0	|0	|0	|0	|0	|0	|0	|0	|0
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
prevNode:  在findGE中进行的更新
0	1	2	3	4	5	6	7	8	9	10	11
---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16
---	--- ---	--- ---	--- ---	--- ---	--- ---	---

三: put k2->2 to  11 levels
kvData:
0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|k	|1	|1	|k	|3	|3	|k	|2	|2	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|	|
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---
nodeData:
/第一个node是header
																/第二个node k1
																																/第三个node k3
																																													/第四个node k2
0	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19	20	21	22	23	24	25	26	27	28	29	30	31	32	33	34	35	36	37	38	39	40	41	42	43	44	45	46	47	48	49	50	51	52	53	54	55	56	57	58	56
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---	--- ---	---	--- ---	--- ---	--- ---	--- ---	---	--- ---	---
|0	|0	|0	|12	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|16	|0	|2	|1	|12	|32	|32	|32	|32	|32	|32	|32	|32	|32	|45	|45	|0	|6	|2	|1	|9	|45	|45	|45	|45	|45	|45	|45	|45	|45	|9	|2	|1	|11	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0	|0
---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	--- ---	---	--- ---	---	--- ---	--- ---	--- ---	--- ---	---	--- ---	---
prevNode:  在findGE中进行的更新
0	1	2	3	4	5	6	7	8	9	10	11
---	--- ---	--- ---	--- ---	--- ---	--- ---	---
|32	|32	|32	|32	|32	|32	|32	|32	|32	|16	|16	|16
---	--- ---	--- ---	--- ---	--- ---	--- ---	---

*/

// DB is an in-memory key/value database.
// memDB
type DB struct {
	cmp comparer.BasicComparer
	rnd *rand.Rand

	mu     sync.RWMutex
	kvData []byte // 记录实际的kv
	// Node data:
	// [0]         : KV offset      记录这个kv在kvData中的起始偏移量
	// [1]         : Key length     记录这个kv在kvData中key的长度
	// [2]         : Value length   记录这个kv在kvData中value的长度
	// [3]         : Height         本节点的所处的高度，从0开始，0——11
	// [3..height] : Next nodes     用来存储每一层对应的下一个节点的【索引】
	nodeData  []int           // 跳表，一个节点包括了上述注释中的所有内容。
	prevNode  [tMaxHeight]int // 当前检索的key所在node在每一层的前一个node，属于临时变量，在find时根据情况进行更新
	maxHeight int             // 跳表最大高度
	n         int             // 跳表的节点总数
	kvSize    int             // kv占用的总空间数
}

// 跳表特性，确保分布在在第1、2、3、4层的概率分别是1/2，1/4，1/8，1/16
func (p *DB) randHeight() (h int) {
	const branching = 4
	h = 1
	for h < tMaxHeight && p.rnd.Int()%branching == 0 {
		h++
	}
	return
}

// Must hold RW-lock if prev == true, as it use shared prevNode slice.
// 从最高层开始找，一直找到最底层，然后返回比key大的下一个node在nodeData中的索引；
// 注意，如果prev是true，则在找的过程中用prevNode记录此key形成的node，在每层高度的前置node（索引）
func (p *DB) findGE(key []byte, prev bool) (int, bool) {
	node := 0
	// 最开始maxHeight是1
	h := p.maxHeight - 1
	for {
		// 从header节点中找到最高层对应的下一个节点，也就是索引15那个位置的值，next是16， 32
		next := p.nodeData[node+nNext+h] // put k1: 0+4+0=4; put k3: 0+4+11=15, 16+4+11=31, 30； put k2: 0+4+11=15, 16+4+11=31, 30,...,30
		cmp := 1
		if next != 0 {
			o := p.nodeData[next]
			cmp = p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key)
		}
		// node中的key小于查询的key，接着向后找
		if cmp < 0 {
			// Keep searching in this list
			// 这里记录此key所在node在每层高度的前置node索引
			node = next // put k3: 16, 32
		} else {
			if prev {
				// 在put和get时，需要找到此key对应node的pre node，进行插入和删除
				p.prevNode[h] = node
			} else if cmp == 0 {
				// node中的key等于查询的key
				return next, true
			}
			// node中的key大于查询的key，下沉到下一层高度继续查找
			// 找到最下面一层原始数据
			if h == 0 {
				return next, cmp == 0
			}
			// 下沉到下一层高度
			h--
		}
	}
}

func (p *DB) findLT(key []byte) int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		o := p.nodeData[next]
		if next == 0 || p.cmp.Compare(p.kvData[o:o+p.nodeData[next+nKey]], key) >= 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

func (p *DB) findLast() int {
	node := 0
	h := p.maxHeight - 1
	for {
		next := p.nodeData[node+nNext+h]
		if next == 0 {
			if h == 0 {
				break
			}
			h--
		} else {
			node = next
		}
	}
	return node
}

// Put sets the value for the given key. It overwrites any previous value
// for that key; a DB is not a multi-map.
//
// It is safe to modify the contents of the arguments after Put returns.
// todo 性能优化 kvData nodeData append优化成创建固定容量的slice？
// key是InternalKey
func (p *DB) Put(key []byte, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// nodeData中已经存在了这个key，则在kvData中追加数据，在nodeData中更新索引和value长度
	// 此时不需要更新其他层级的索引
	if node, exact := p.findGE(key, true); exact {
		kvOffset := len(p.kvData)
		p.kvData = append(p.kvData, key...)
		p.kvData = append(p.kvData, value...)
		p.nodeData[node] = kvOffset
		m := p.nodeData[node+nVal]
		p.nodeData[node+nVal] = len(value)
		p.kvSize += len(value) - m
		return nil
	}

	// 按照特定概率，"随机"选在要插入的高度
	h := p.randHeight()
	if h > p.maxHeight {
		for i := p.maxHeight; i < h; i++ {
			p.prevNode[i] = 0
		}
		p.maxHeight = h
	}
	// 追加数据到kvData
	kvOffset := len(p.kvData)
	p.kvData = append(p.kvData, key...)
	p.kvData = append(p.kvData, value...)
	// 追加node到nodeData中
	// 最新的node
	node := len(p.nodeData) // put k1:16; put k3:32; put k2:45
	p.nodeData = append(p.nodeData, kvOffset, len(key), len(value), h)
	for i, n := range p.prevNode[:h] {
		m := n + nNext + i // 0 4 0,   11; 16+4+0, 8；32+4+0
		// 给新node添加0-h层高度，对应的下一个节点的【索引】；就是前一个node中原来指向的那些节点
		p.nodeData = append(p.nodeData, p.nodeData[m])
		// 更新前一个node的0-h层高度，对应的下一个节点的索引；就是这个新node
		p.nodeData[m] = node
	}
	// 更新kvSize，元素个数++
	p.kvSize += len(key) + len(value)
	p.n++
	return nil
}

// Delete deletes the value for the given key. It returns ErrNotFound if
// the DB does not contain the key.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (p *DB) Delete(key []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	node, exact := p.findGE(key, true)
	if !exact {
		return ErrNotFound
	}

	h := p.nodeData[node+nHeight]
	for i, n := range p.prevNode[:h] {
		m := n + nNext + i
		p.nodeData[m] = p.nodeData[p.nodeData[m]+nNext+i]
	}

	p.kvSize -= p.nodeData[node+nKey] + p.nodeData[node+nVal]
	p.n--
	return nil
}

// Contains returns true if the given key are in the DB.
//
// It is safe to modify the contents of the arguments after Contains returns.
func (p *DB) Contains(key []byte) bool {
	p.mu.RLock()
	_, exact := p.findGE(key, false)
	p.mu.RUnlock()
	return exact
}

// Get gets the value for the given key. It returns error.ErrNotFound if the
// DB does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (p *DB) Get(key []byte) (value []byte, err error) {
	p.mu.RLock()
	if node, exact := p.findGE(key, false); exact {
		o := p.nodeData[node] + p.nodeData[node+nKey]
		value = p.kvData[o : o+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// Find finds key/value pair whose key is greater than or equal to the
// given key. It returns ErrNotFound if the table doesn't contain
// such pair.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Find returns.
func (p *DB) Find(key []byte) (rkey, value []byte, err error) {
	p.mu.RLock()
	if node, _ := p.findGE(key, false); node != 0 {
		n := p.nodeData[node]
		m := n + p.nodeData[node+nKey]
		rkey = p.kvData[n:m]
		value = p.kvData[m : m+p.nodeData[node+nVal]]
	} else {
		err = ErrNotFound
	}
	p.mu.RUnlock()
	return
}

// NewIterator returns an iterator of the DB.
// The returned iterator is not safe for concurrent use, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
// It is also safe to use an iterator concurrently with modifying its
// underlying DB. However, the resultant key/value pairs are not guaranteed
// to be a consistent snapshot of the DB at a particular point in time.
//
// Slice allows slicing the iterator to only contains keys in the given
// range. A nil Range.Start is treated as a key before all keys in the
// DB. And a nil Range.Limit is treated as a key after all keys in
// the DB.
//
// WARNING: Any slice returned by interator (e.g. slice returned by calling
// Iterator.Key() or Iterator.Key() methods), its content should not be modified
// unless noted otherwise.
//
// The iterator must be released after use, by calling Release method.
//
// Also read Iterator documentation of the leveldb/iterator package.
func (p *DB) NewIterator(slice *util.Range) iterator.Iterator {
	return &dbIter{p: p, slice: slice}
}

// Capacity returns keys/values buffer capacity.
func (p *DB) Capacity() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData)
}

// Size returns sum of keys and values length. Note that deleted
// key/value will not be accounted for, but it will still consume
// the buffer, since the buffer is append only.
func (p *DB) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.kvSize
}

// Free returns keys/values free buffer before need to grow.
func (p *DB) Free() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return cap(p.kvData) - len(p.kvData)
}

// Len returns the number of entries in the DB.
func (p *DB) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.n
}

// Reset resets the DB to initial empty state. Allows reuse the buffer.
func (p *DB) Reset() {
	p.mu.Lock()
	p.rnd = rand.New(rand.NewSource(0xdeadbeef))
	p.maxHeight = 1
	p.n = 0
	p.kvSize = 0
	p.kvData = p.kvData[:0]
	p.nodeData = p.nodeData[:nNext+tMaxHeight] // 最前面的是高层的
	p.nodeData[nKV] = 0
	p.nodeData[nKey] = 0
	p.nodeData[nVal] = 0
	p.nodeData[nHeight] = tMaxHeight
	for n := 0; n < tMaxHeight; n++ {
		p.nodeData[nNext+n] = 0
		p.prevNode[n] = 0
	}
	p.mu.Unlock()
}

// New creates a new initialized in-memory key/value DB. The capacity
// is the initial key/value buffer capacity. The capacity is advisory,
// not enforced.
//
// This DB is append-only, deleting an entry would remove entry node but not
// reclaim KV buffer.
//
// The returned DB instance is safe for concurrent use.
// capacity 默认4M
func New(cmp comparer.BasicComparer, capacity int) *DB {
	p := &DB{
		cmp:       cmp,
		rnd:       rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		kvData:    make([]byte, 0, capacity),
		nodeData:  make([]int, 4+tMaxHeight),
	}
	p.nodeData[nHeight] = tMaxHeight
	return p
}

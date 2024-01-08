// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sort"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	undefinedCompaction = iota
	level0Compaction
	nonLevel0Compaction
	seekCompaction
)

func (s *session) pickMemdbLevel(umin, umax []byte, maxLevel int) int {
	v := s.version()
	defer v.release()
	return v.pickMemdbLevel(umin, umax, maxLevel)
}

// 把frozenMemDB中的kv，按序写入到sst中（构建data block，index block, filter block)
// 并将flushLevel加入到 sessionRecord.addedTables
func (s *session) flushMemdb(rec *sessionRecord, mdb *memdb.DB, maxLevel int) (int, error) {
	// Create sorted table.
	iter := mdb.NewIterator(nil)
	defer iter.Release()
	// 构造sst，并返回tFile
	// 把kvs写入data block, 同时会把对应的index写入index block，布隆过滤器写入filter block
	// sst文件刷盘（默认不刷盘）
	t, n, err := s.tops.createFrom(iter)
	if err != nil {
		return 0, err
	}

	// Pick level other than zero can cause compaction issue with large
	// bulk insert and delete on strictly incrementing key-space. The
	// problem is that the small deletion markers trapped at lower level,
	// while key/value entries keep growing at higher level. Since the
	// key-space is strictly incrementing it will not overlaps with
	// higher level, thus maximum possible level is always picked, while
	// overlapping deletion marker pushed into lower level.
	// See: https://github.com/syndtr/goleveldb/issues/127.
	// 传进来的是刚刚写入level0层的sst
	// 如果这个sst跟level 0中其他sst文件不存在重叠，且和level i中的sst文件也不存在冲突，且跟level i+1中的sst文件重叠不超过10个，则返回i
	// 表示将这个新的sst直接写入level i，在进行table compaction时会进行处理
	flushLevel := s.pickMemdbLevel(t.imin.ukey(), t.imax.ukey(), maxLevel)
	rec.addTableFile(flushLevel, t)

	s.logf("memdb@flush created L%d@%d N·%d S·%s %q:%q", flushLevel, t.fd.Num, n, shortenb(t.size), t.imin, t.imax)
	return flushLevel, nil
}

// Pick a compaction based on current state; need external synchronization.
// 找出来需要进行table compaction的level和sst文件，构造并返回compaction
func (s *session) pickCompaction() *compaction {
	v := s.version()

	var sourceLevel int
	var t0 tFiles
	var typ int
	// 有因为level 0层文件数过多或者level i层文件总量过大引起的compaction的sst
	if v.cScore >= 1 {
		sourceLevel = v.cLevel
		cptr := s.getCompPtr(sourceLevel) // imax
		tables := v.levels[sourceLevel]
		// 非level 0层，找到需要compaction的那个sst
		if cptr != nil && sourceLevel > 0 {
			n := len(tables)
			// 基于上次进行table compaction的sst的最大key,找到下一个最近的sst索引
			if i := sort.Search(n, func(i int) bool {
				return s.icmp.Compare(tables[i].imax, cptr) > 0
			}); i < n {
				t0 = append(t0, tables[i])
			}
		}
		// 如果非level 0层，没有需要进行compaction的sst，则把level 0层的sst放入
		if len(t0) == 0 {
			t0 = append(t0, tables[0])
		}
		if sourceLevel == 0 {
			typ = level0Compaction
		} else {
			typ = nonLevel0Compaction
		}
	} else {
		// 剩下的就是因为多次无效查询而引起的seek类型的compaction的sst
		if p := atomic.LoadPointer(&v.cSeek); p != nil {
			ts := (*tSet)(p)
			sourceLevel = ts.level
			t0 = append(t0, ts.table)
			typ = seekCompaction
		} else {
			v.release()
			return nil
		}
	}
	// 创建并返回compaction
	return newCompaction(s, v, sourceLevel, t0, typ)
}

// Create compaction from given level and range; need external synchronization.
func (s *session) getCompactionRange(sourceLevel int, umin, umax []byte, noLimit bool) *compaction {
	v := s.version()

	if sourceLevel >= len(v.levels) {
		v.release()
		return nil
	}

	t0 := v.levels[sourceLevel].getOverlaps(nil, s.icmp, umin, umax, sourceLevel == 0)
	if len(t0) == 0 {
		v.release()
		return nil
	}

	// Avoid compacting too much in one shot in case the range is large.
	// But we cannot do this for level-0 since level-0 files can overlap
	// and we must not pick one file and drop another older file if the
	// two files overlap.
	if !noLimit && sourceLevel > 0 {
		limit := int64(v.s.o.GetCompactionSourceLimit(sourceLevel))
		total := int64(0)
		for i, t := range t0 {
			total += t.size
			if total >= limit {
				s.logf("table@compaction limiting F·%d -> F·%d", len(t0), i+1)
				t0 = t0[:i+1]
				break
			}
		}
	}

	typ := level0Compaction
	if sourceLevel != 0 {
		typ = nonLevel0Compaction
	}
	return newCompaction(s, v, sourceLevel, t0, typ)
}

// 创建并返回compaction
func newCompaction(s *session, v *version, sourceLevel int, t0 tFiles, typ int) *compaction {
	c := &compaction{
		s:             s,
		v:             v,
		typ:           typ,
		sourceLevel:   sourceLevel,
		levels:        [2]tFiles{t0, nil},
		maxGPOverlaps: int64(s.o.GetCompactionGPOverlaps(sourceLevel)), // 10*2MB*1^(level+2) = 20MB
		tPtrs:         make([]int, len(v.levels)),
	}
	// 计算source层和source+1层受此次compaction影响的ssts, imin,imax，并赋值给companion.levels
	// 计算source+2层受此次compaction影响的sst，并赋值给compaction.gp
	// 注意对于source是level 0时，一次可能把多个0层的sst整理到1层，但是对于非0层，一次只会把1个ssts整理到下一层
	c.expand()

	// ？看起来是跟snap相关的信息
	c.save()
	return c
}

// compaction represent a compaction state.
type compaction struct {
	s *session
	v *version

	typ           int       // table compaction的三种类型
	sourceLevel   int       // 本次进行compaction的层
	levels        [2]tFiles // 0位置是source层需要进行compaction的ssts, 1位置是source+1层被此次compaction影响的ssts
	maxGPOverlaps int64     // 10*2MB*1^(level+2) = 20MB

	gp                tFiles // version中下两层（grandparent）中跟此次compaction影响范围的amin，amax重叠的部分
	gpi               int
	seenKey           bool
	gpOverlappedBytes int64
	imin, imax        internalKey // source层进行compaction的ssts中，最小和最大的key
	tPtrs             []int
	released          bool

	snapGPI               int
	snapSeenKey           bool
	snapGPOverlappedBytes int64
	snapTPtrs             []int
}

func (c *compaction) save() {
	c.snapGPI = c.gpi
	c.snapSeenKey = c.seenKey
	c.snapGPOverlappedBytes = c.gpOverlappedBytes
	c.snapTPtrs = append(c.snapTPtrs[:0], c.tPtrs...)
}

func (c *compaction) restore() {
	c.gpi = c.snapGPI
	c.seenKey = c.snapSeenKey
	c.gpOverlappedBytes = c.snapGPOverlappedBytes
	c.tPtrs = append(c.tPtrs[:0], c.snapTPtrs...)
}

func (c *compaction) release() {
	if !c.released {
		c.released = true
		c.v.release()
	}
}

// Expand compacted tables; need external synchronization.
// 计算source层和source+1层受此次compaction影响的ssts, imin,imax，并赋值给companion.levels
// 计算source+2层受此次compaction影响的sst，并赋值给compaction.gp
// 注意对于source是level 0时，一次可能把多个0层的sst整理到1层，但是对于非0层，一次只会把1个ssts整理到下一层
func (c *compaction) expand() {
	// 默认50MB
	limit := int64(c.s.o.GetCompactionExpandLimit(c.sourceLevel)) // 25*（2MB*1^level）= 50MB*1^level = 50MB
	// version中需要进行compaction的source层和source+1层的tFiles
	vt0 := c.v.levels[c.sourceLevel]
	vt1 := tFiles{}
	if level := c.sourceLevel + 1; level < len(c.v.levels) {
		vt1 = c.v.levels[level]
	}

	// compaction中需要进行compaction的source层和source+1层的tFiles
	t0, t1 := c.levels[0], c.levels[1]
	imin, imax := t0.getRange(c.s.icmp)

	// For non-zero levels, the ukey can't hop across tables at all.
	// 对0层某个sst进行compaction的话，因为sst间的key有重叠，需要检索并返回0层中跟umin和umax重叠的所有sst文件
	// 并基于新的ssts，算出来新的imin, imax
	if c.sourceLevel == 0 {
		// We expand t0 here just incase ukey hop across tables.
		t0 = vt0.getOverlaps(t0, c.s.icmp, imin.ukey(), imax.ukey(), c.sourceLevel == 0)
		if len(t0) != len(c.levels[0]) {
			imin, imax = t0.getRange(c.s.icmp)
		}
	}
	// source+1层跟imin,imax重叠的部分
	t1 = vt1.getOverlaps(t1, c.s.icmp, imin.ukey(), imax.ukey(), false)
	// Get entire range covered by compaction.
	// 两层部分sst合并后的新的范围
	amin, amax := append(t0, t1...).getRange(c.s.icmp)

	// See if we can grow the number of inputs in "sourceLevel" without
	// changing the number of "sourceLevel+1" files we pick up.
	// 如果source层和source+1层有重叠部分
	// 这里会基于两层合并后新的范围（amin, amax），去source层判断一下是不是有更大的重叠范围
	// 如果有更大的范围，但是两层重叠部分ssts的大小没超过limit，则会尝试把source层更多的ssts compaction到source+1层
	// 但是只有在不增加source+1层影响范围的情况下才会实际这么做
	if len(t1) > 0 {
		exp0 := vt0.getOverlaps(nil, c.s.icmp, amin.ukey(), amax.ukey(), c.sourceLevel == 0)
		if len(exp0) > len(t0) && t1.size()+exp0.size() < limit {
			xmin, xmax := exp0.getRange(c.s.icmp)
			exp1 := vt1.getOverlaps(nil, c.s.icmp, xmin.ukey(), xmax.ukey(), false)
			if len(exp1) == len(t1) {
				c.s.logf("table@compaction expanding L%d+L%d (F·%d S·%s)+(F·%d S·%s) -> (F·%d S·%s)+(F·%d S·%s)",
					c.sourceLevel, c.sourceLevel+1, len(t0), shortenb(t0.size()), len(t1), shortenb(t1.size()),
					len(exp0), shortenb(exp0.size()), len(exp1), shortenb(exp1.size()))
				imin, imax = xmin, xmax
				t0, t1 = exp0, exp1
				amin, amax = append(t0, t1...).getRange(c.s.icmp)
			}
		}
	}

	// Compute the set of grandparent files that overlap this compaction
	// (parent == sourceLevel+1; grandparent == sourceLevel+2)
	// version中下两层中跟此次compaction影响范围的amin，amax重叠的部分
	if level := c.sourceLevel + 2; level < len(c.v.levels) {
		c.gp = c.v.levels[level].getOverlaps(c.gp, c.s.icmp, amin.ukey(), amax.ukey(), false)
	}

	// 将source层和source+1层受影响的ssts加入到compaction.levels中
	// 并更新最小和最大key
	c.levels[0], c.levels[1] = t0, t1
	c.imin, c.imax = imin, imax
}

// Check whether compaction is trivial.
func (c *compaction) trivial() bool {
	return len(c.levels[0]) == 1 && len(c.levels[1]) == 0 && c.gp.size() <= c.maxGPOverlaps
}

func (c *compaction) baseLevelForKey(ukey []byte) bool {
	for level := c.sourceLevel + 2; level < len(c.v.levels); level++ {
		tables := c.v.levels[level]
		for c.tPtrs[level] < len(tables) {
			t := tables[c.tPtrs[level]]
			if c.s.icmp.uCompare(ukey, t.imax.ukey()) <= 0 {
				// We've advanced far enough.
				if c.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					// Key falls in this file's range, so definitely not base level.
					return false
				}
				break
			}
			c.tPtrs[level]++
		}
	}
	return true
}

func (c *compaction) shouldStopBefore(ikey internalKey) bool {
	for ; c.gpi < len(c.gp); c.gpi++ {
		gp := c.gp[c.gpi]
		if c.s.icmp.Compare(ikey, gp.imax) <= 0 {
			break
		}
		if c.seenKey {
			c.gpOverlappedBytes += gp.size
		}
	}
	c.seenKey = true

	if c.gpOverlappedBytes > c.maxGPOverlaps {
		// Too much overlap for current output; start new output.
		c.gpOverlappedBytes = 0
		return true
	}
	return false
}

// Creates an iterator.
// 基于source层和source+1层中需要compaction的sst，构造一个mergedIterator
// 该迭代器可以把多个sst中的kv，整体按序迭代出来
func (c *compaction) newIterator() iterator.Iterator {
	// Creates iterator slice.
	icap := len(c.levels)
	if c.sourceLevel == 0 {
		// Special case for level-0.
		icap = len(c.levels[0]) + 1
	}
	its := make([]iterator.Iterator, 0, icap)

	// Options.
	ro := &opt.ReadOptions{
		DontFillCache: true,
		Strict:        opt.StrictOverride,
	}
	strict := c.s.o.GetStrict(opt.StrictCompaction)
	if strict {
		ro.Strict |= opt.StrictReader
	}

	// 基于source层和source+1层中需要compaction的sst
	// 给每个sst构造indexedIterator迭代器，indexedIterator按序迭代sst中每个data block中的kv
	// 注意，sst内的k本身就是按序的
	for i, tables := range c.levels {
		if len(tables) == 0 {
			continue
		}

		// Level-0 is not sorted and may overlaps each other.
		if c.sourceLevel+i == 0 {
			for _, t := range tables {
				its = append(its, c.s.tops.newIterator(t, nil, ro))
			}
		} else {
			it := iterator.NewIndexedIterator(tables.newIndexIterator(c.s.tops, c.s.icmp, nil, ro), strict)
			its = append(its, it)
		}
	}

	// 基于每个sst的迭代器，构造一个mergedIterator
	// 该迭代器可以把多个sst中的kv，整体按序迭代出来
	return iterator.NewMergedIterator(its, c.s.icmp, strict)
}

// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errCompactionTransactExiting = errors.New("leveldb: compaction transact exiting")
)

type cStat struct {
	duration time.Duration
	read     int64
	write    int64
}

func (p *cStat) add(n *cStatStaging) {
	p.duration += n.duration
	p.read += n.read
	p.write += n.write
}

func (p *cStat) get() (duration time.Duration, read, write int64) {
	return p.duration, p.read, p.write
}

type cStatStaging struct {
	start    time.Time
	duration time.Duration
	on       bool
	read     int64
	write    int64
}

func (p *cStatStaging) startTimer() {
	if !p.on {
		p.start = time.Now()
		p.on = true
	}
}

func (p *cStatStaging) stopTimer() {
	if p.on {
		p.duration += time.Since(p.start)
		p.on = false
	}
}

type cStats struct {
	lk    sync.Mutex
	stats []cStat
}

func (p *cStats) addStat(level int, n *cStatStaging) {
	p.lk.Lock()
	if level >= len(p.stats) {
		newStats := make([]cStat, level+1)
		copy(newStats, p.stats)
		p.stats = newStats
	}
	p.stats[level].add(n)
	p.lk.Unlock()
}

func (p *cStats) getStat(level int) (duration time.Duration, read, write int64) {
	p.lk.Lock()
	defer p.lk.Unlock()
	if level < len(p.stats) {
		return p.stats[level].get()
	}
	return
}

func (db *DB) compactionError() {
	var err error
noerr:
	// No error.
	for {
		select {
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
				goto haserr
			}
		case <-db.closeC:
			return
		}
	}
haserr:
	// Transient error.
	for {
		select {
		case db.compErrC <- err:
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
				goto noerr
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
			}
		case <-db.closeC:
			return
		}
	}
hasperr:
	// Persistent error.
	for {
		select {
		case db.compErrC <- err:
		case db.compPerErrC <- err:
		case db.writeLockC <- struct{}{}:
			// Hold write lock, so that write won't pass-through.
			db.compWriteLocking = true
		case <-db.closeC:
			if db.compWriteLocking {
				// We should release the lock or Close will hang.
				<-db.writeLockC
			}
			return
		}
	}
}

type compactionTransactCounter int

func (cnt *compactionTransactCounter) incr() {
	*cnt++
}

type compactionTransactInterface interface {
	run(cnt *compactionTransactCounter) error
	revert() error
}

func (db *DB) compactionTransact(name string, t compactionTransactInterface) {
	defer func() {
		if x := recover(); x != nil {
			if x == errCompactionTransactExiting {
				if err := t.revert(); err != nil {
					db.logf("%s revert error %q", name, err)
				}
			}
			panic(x)
		}
	}()

	const (
		backoffMin = 1 * time.Second
		backoffMax = 8 * time.Second
		backoffMul = 2 * time.Second
	)
	var (
		backoff  = backoffMin
		backoffT = time.NewTimer(backoff)
		lastCnt  = compactionTransactCounter(0)

		disableBackoff = db.s.o.GetDisableCompactionBackoff()
	)
	for n := 0; ; n++ {
		// Check whether the DB is closed.
		if db.isClosed() {
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		} else if n > 0 {
			db.logf("%s retrying N·%d", name, n)
		}

		// Execute.
		cnt := compactionTransactCounter(0)
		err := t.run(&cnt)
		if err != nil {
			db.logf("%s error I·%d %q", name, cnt, err)
		}

		// Set compaction error status.
		select {
		case db.compErrSetC <- err:
		case perr := <-db.compPerErrC:
			if err != nil {
				db.logf("%s exiting (persistent error %q)", name, perr)
				db.compactionExitTransact()
			}
		case <-db.closeC:
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		}
		if err == nil {
			return
		}
		if errors.IsCorrupted(err) {
			db.logf("%s exiting (corruption detected)", name)
			db.compactionExitTransact()
		}

		if !disableBackoff {
			// Reset backoff duration if counter is advancing.
			if cnt > lastCnt {
				backoff = backoffMin
				lastCnt = cnt
			}

			// Backoff.
			backoffT.Reset(backoff)
			if backoff < backoffMax {
				backoff *= backoffMul
				if backoff > backoffMax {
					backoff = backoffMax
				}
			}
			select {
			case <-backoffT.C:
			case <-db.closeC:
				db.logf("%s exiting", name)
				db.compactionExitTransact()
			}
		}
	}
}

type compactionTransactFunc struct {
	runFunc    func(cnt *compactionTransactCounter) error
	revertFunc func() error
}

func (t *compactionTransactFunc) run(cnt *compactionTransactCounter) error {
	return t.runFunc(cnt)
}

func (t *compactionTransactFunc) revert() error {
	if t.revertFunc != nil {
		return t.revertFunc()
	}
	return nil
}

func (db *DB) compactionTransactFunc(name string, run func(cnt *compactionTransactCounter) error, revert func() error) {
	db.compactionTransact(name, &compactionTransactFunc{run, revert})
}

func (db *DB) compactionExitTransact() {
	panic(errCompactionTransactExiting)
}

// 把session和new version中的信息更新到sessionRecord中, 并把sessionRecord写入manifest
// 并用new version更新session中的current version
func (db *DB) compactionCommit(name string, rec *sessionRecord) {
	db.compCommitLk.Lock()
	defer db.compCommitLk.Unlock() // Defer is necessary.
	db.compactionTransactFunc(name+"@commit", func(cnt *compactionTransactCounter) error {
		return db.s.commit(rec, true)
	}, nil)
}

// 把frozenMemDB中的kv，按序写入到sst中（构建data block，index block, filter block)
// 并将flushLevel加入到 sessionRecord.addedTables
// 把session和new version中的信息更新到sessionRecord中, 并把sessionRecord写入manifest
// 并用new version更新session中的current version
// 清除frozen memdb和对应的journal
// 非阻塞式的触发table compaction
func (db *DB) memCompaction() {
	mdb := db.getFrozenMem()
	if mdb == nil {
		return
	}
	defer mdb.decref()

	db.logf("memdb@flush N·%d S·%s", mdb.Len(), shortenb(int64(mdb.Size())))

	// Don't compact empty memdb.
	if mdb.Len() == 0 {
		db.logf("memdb@flush skipping")
		// drop frozen memdb
		db.dropFrozenMem()
		return
	}

	// Pause table compaction.
	// 如果正在进行table compaction，需要等table compaction进行完后，才能再进行mem compaction
	// 如果正好要准备进行table compaction，则先让其停止，先进行mem compaction
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseC <- (chan<- struct{})(resumeC):
	case <-db.compPerErrC:
		close(resumeC)
		resumeC = nil
	case <-db.closeC:
		db.compactionExitTransact()
	}

	var (
		rec        = &sessionRecord{}
		stats      = &cStatStaging{}
		flushLevel int
	)

	// Generate tables.
	db.compactionTransactFunc("memdb@flush", func(cnt *compactionTransactCounter) (err error) {
		// 把frozenMemDB中的kv，按序写入到sst中（构建data block，index block, filter block)
		// 并将flushLevel加入到 sessionRecord.addedTables
		stats.startTimer()
		flushLevel, err = db.s.flushMemdb(rec, mdb.DB, db.memdbMaxLevel)
		stats.stopTimer()
		return
	}, func() error {
		// 创建sst失败，则撤销增加的sst
		for _, r := range rec.addedTables {
			db.logf("memdb@flush revert @%d", r.num)
			if err := db.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: r.num}); err != nil {
				return err
			}
		}
		return nil
	})

	// 给record.seqNum赋值
	rec.setJournalNum(db.journalFd.Num)
	rec.setSeqNum(db.frozenSeq)

	// Commit.
	// 把session和new version中的信息更新到sessionRecord中, 并把sessionRecord写入manifest
	// 并用new version更新session中的current version
	stats.startTimer()
	db.compactionCommit("memdb", rec)
	stats.stopTimer()

	db.logf("memdb@flush committed F·%d T·%v", len(rec.addedTables), stats.duration)

	// Save compaction stats
	for _, r := range rec.addedTables {
		stats.write += r.size
	}
	db.compStats.addStat(flushLevel, stats)
	atomic.AddUint32(&db.memComp, 1)

	// Drop frozen memdb.
	// 清除frozen memdb和对应的journal
	db.dropFrozenMem()

	// Resume table compaction.
	if resumeC != nil {
		select {
		case <-resumeC:
			close(resumeC)
		case <-db.closeC:
			db.compactionExitTransact()
		}
	}

	// Trigger table compaction.
	// 非阻塞式的触发table compaction
	db.compTrigger(db.tcompCmdC)
}

type tableCompactionBuilder struct {
	db    *DB
	s     *session
	c     *compaction
	rec   *sessionRecord
	stat1 *cStatStaging

	snapHasLastUkey bool
	snapLastUkey    []byte
	snapLastSeq     uint64
	snapIter        int
	snapKerrCnt     int
	snapDropCnt     int

	kerrCnt int
	dropCnt int

	minSeq    uint64
	strict    bool
	tableSize int

	tw *tWriter
}

// 把数据写入data block，并将索引和key写入index block和filer block
func (b *tableCompactionBuilder) appendKV(key, value []byte) error {
	// Create new table if not already.
	if b.tw == nil {
		// Check for pause event.
		if b.db != nil {
			select {
			case ch := <-b.db.tcompPauseC:
				b.db.pauseCompaction(ch)
			case <-b.db.closeC:
				b.db.compactionExitTransact()
			default:
			}
		}

		// Create new table.
		var err error
		b.tw, err = b.s.tops.create(b.tableSize)
		if err != nil {
			return err
		}
	}

	// Write key/value into table.
	// 把数据写入data block，并将索引和key写入index block和filer block
	return b.tw.append(key, value)
}

func (b *tableCompactionBuilder) needFlush() bool {
	return b.tw.tw.BytesLen() >= b.tableSize
}

func (b *tableCompactionBuilder) flush() error {
	// sst文件刷盘（默认不刷盘），并构建tFile
	t, err := b.tw.finish()
	if err != nil {
		return err
	}
	// 将加入到source+1层的tFile文件添加进sessionRecord中
	b.rec.addTableFile(b.c.sourceLevel+1, t)
	b.stat1.write += t.size
	b.s.logf("table@build created L%d@%d N·%d S·%s %q:%q", b.c.sourceLevel+1, t.fd.Num, b.tw.tw.EntriesLen(), shortenb(t.size), t.imin, t.imax)
	b.tw = nil
	return nil
}

func (b *tableCompactionBuilder) cleanup() error {
	if b.tw != nil {
		if err := b.tw.drop(); err != nil {
			return err
		}
		b.tw = nil
	}
	return nil
}

// 基于source层和source+1层中需要compaction的sst，构造一个mergedIterator
// 把数据写入新的data block，并将索引和key写入index block和filer block
// sst文件刷盘（默认不刷盘），并构建tFile
// 将加入到source+1层的tFile文件添加进sessionRecord中
func (b *tableCompactionBuilder) run(cnt *compactionTransactCounter) (err error) {
	snapResumed := b.snapIter > 0
	hasLastUkey := b.snapHasLastUkey // The key might has zero length, so this is necessary.
	lastUkey := append([]byte(nil), b.snapLastUkey...)
	lastSeq := b.snapLastSeq
	b.kerrCnt = b.snapKerrCnt
	b.dropCnt = b.snapDropCnt
	// Restore compaction state.
	b.c.restore()

	defer func() {
		if cerr := b.cleanup(); cerr != nil {
			if err == nil {
				err = cerr
			} else {
				err = fmt.Errorf("tableCompactionBuilder error: %v, cleanup error (%v)", err, cerr)
			}
		}
	}()

	b.stat1.startTimer()
	defer b.stat1.stopTimer()

	// 基于source层和source+1层中需要compaction的sst，构造一个mergedIterator
	// 该迭代器可以把多个sst中的kv，整体按序迭代出来
	iter := b.c.newIterator()
	defer iter.Release()
	for i := 0; iter.Next(); i++ {
		// Incr transact counter.
		cnt.incr()

		// Skip until last state.
		if i < b.snapIter {
			continue
		}

		resumed := false
		if snapResumed {
			resumed = true
			snapResumed = false
		}

		// 获取key
		ikey := iter.Key()
		ukey, seq, kt, kerr := parseInternalKey(ikey)

		if kerr == nil {
			shouldStop := !resumed && b.c.shouldStopBefore(ikey)

			if !hasLastUkey || b.s.icmp.uCompare(lastUkey, ukey) != 0 {
				// First occurrence of this user key.

				// Only rotate tables if ukey doesn't hop across.
				if b.tw != nil && (shouldStop || b.needFlush()) {
					if err := b.flush(); err != nil {
						return err
					}

					// Creates snapshot of the state.
					b.c.save()
					b.snapHasLastUkey = hasLastUkey
					b.snapLastUkey = append(b.snapLastUkey[:0], lastUkey...)
					b.snapLastSeq = lastSeq
					b.snapIter = i
					b.snapKerrCnt = b.kerrCnt
					b.snapDropCnt = b.dropCnt
				}

				hasLastUkey = true
				lastUkey = append(lastUkey[:0], ukey...)
				lastSeq = keyMaxSeq
			}

			switch {
			case lastSeq <= b.minSeq:
				// Dropped because newer entry for same user key exist
				fallthrough // (A)
			case kt == keyTypeDel && seq <= b.minSeq && b.c.baseLevelForKey(lastUkey):
				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger seq numbers
				// (3) data in layers that are being compacted here and have
				//     smaller seq numbers will be dropped in the next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				lastSeq = seq
				b.dropCnt++
				continue
			default:
				lastSeq = seq
			}
		} else {
			if b.strict {
				return kerr
			}

			// Don't drop corrupted keys.
			hasLastUkey = false
			lastUkey = lastUkey[:0]
			lastSeq = keyMaxSeq
			b.kerrCnt++
		}

		// 把数据写入data block，并将索引和key写入index block和filer block
		if err := b.appendKV(ikey, iter.Value()); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// Finish last table.
	// sst文件刷盘（默认不刷盘），并构建tFile
	// 将加入到source+1层的tFile文件添加进sessionRecord中
	if b.tw != nil && !b.tw.empty() {
		return b.flush()
	}
	return nil
}

func (b *tableCompactionBuilder) revert() error {
	for _, at := range b.rec.addedTables {
		b.s.logf("table@build revert @%d", at.num)
		if err := b.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: at.num}); err != nil {
			return err
		}
	}
	return nil
}

// table compaction的核心处理流程
func (db *DB) tableCompaction(c *compaction, noTrivial bool) {
	defer c.release()

	rec := &sessionRecord{}
	rec.addCompPtr(c.sourceLevel, c.imax)

	// ppt60中的情况
	// source层只有1个待compaction的sst
	// source+1层没有重叠的文件
	// source+2层跟source层重叠的文件大小不超过20MB
	// 则直接将source层的那个sst移动到source+1层
	// 就return了
	if !noTrivial && c.trivial() {
		t := c.levels[0][0]
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.sourceLevel+1)
		rec.delTable(c.sourceLevel, t.fd.Num)
		rec.addTableFile(c.sourceLevel+1, t)
		db.compactionCommit("table-move", rec)
		return
	}

	// source层和source+1层受本次compaction影响，需要删除的ssts
	var stats [2]cStatStaging
	for i, tables := range c.levels {
		for _, t := range tables {
			stats[i].read += t.size
			// Insert deleted tables into record
			rec.delTable(c.sourceLevel+i, t.fd.Num)
		}
	}
	sourceSize := stats[0].read + stats[1].read
	minSeq := db.minSeq()
	db.logf("table@compaction L%d·%d -> L%d·%d S·%s Q·%d", c.sourceLevel, len(c.levels[0]), c.sourceLevel+1, len(c.levels[1]), shortenb(sourceSize), minSeq)

	b := &tableCompactionBuilder{
		db:        db,
		s:         db.s,
		c:         c,
		rec:       rec,
		stat1:     &stats[1], // source+1层需要read的size
		minSeq:    minSeq,    // snap中最小的seq
		strict:    db.s.o.GetStrict(opt.StrictCompaction),
		tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1), // 2MB
	}
	// 基于source层和source+1层中需要compaction的sst，构造一个mergedIterator
	// 把数据写入新的data block，并将索引和key写入index block和filer block
	// sst文件刷盘（默认不刷盘），并构建tFile
	// 将加入到source+1层的tFile文件添加进sessionRecord中
	db.compactionTransact("table@build", b)

	// Commit.
	// 把session和new version中的信息更新到sessionRecord中, 并把sessionRecord写入manifest
	// 并用new version更新session中的current version
	stats[1].startTimer()
	db.compactionCommit("table", rec)
	stats[1].stopTimer()

	resultSize := stats[1].write
	db.logf("table@compaction committed F%s S%s Ke·%d D·%d T·%v", sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), b.kerrCnt, b.dropCnt, stats[1].duration)

	// Save compaction stats
	for i := range stats {
		db.compStats.addStat(c.sourceLevel+1, &stats[i])
	}
	switch c.typ {
	case level0Compaction:
		atomic.AddUint32(&db.level0Comp, 1)
	case nonLevel0Compaction:
		atomic.AddUint32(&db.nonLevel0Comp, 1)
	case seekCompaction:
		atomic.AddUint32(&db.seekComp, 1)
	}
}

func (db *DB) tableRangeCompaction(level int, umin, umax []byte) error {
	db.logf("table@compaction range L%d %q:%q", level, umin, umax)
	if level >= 0 {
		if c := db.s.getCompactionRange(level, umin, umax, true); c != nil {
			db.tableCompaction(c, true)
		}
	} else {
		// Retry until nothing to compact.
		for {
			compacted := false

			// Scan for maximum level with overlapped tables.
			v := db.s.version()
			m := 1
			for i := m; i < len(v.levels); i++ {
				tables := v.levels[i]
				if tables.overlaps(db.s.icmp, umin, umax, false) {
					m = i
				}
			}
			v.release()

			for level := 0; level < m; level++ {
				if c := db.s.getCompactionRange(level, umin, umax, false); c != nil {
					db.tableCompaction(c, true)
					compacted = true
				}
			}

			if !compacted {
				break
			}
		}
	}

	return nil
}

func (db *DB) tableAutoCompaction() {
	// 找出来需要进行table compaction的level和sst文件，构造并返回compaction
	if c := db.s.pickCompaction(); c != nil {
		// table compaction的核心处理流程，把source和source+1层的sst文件合并，并写入到source+1层
		// 然后把sessionRecord写入manifest，并用new version更新session中的current version
		db.tableCompaction(c, false)
	}
}

func (db *DB) tableNeedCompaction() bool {
	v := db.s.version()
	defer v.release()
	return v.needCompaction()
}

// resumeWrite returns an indicator whether we should resume write operation if enough level0 files are compacted.
func (db *DB) resumeWrite() bool {
	v := db.s.version()
	defer v.release()
	return v.tLen(0) < db.s.o.GetWriteL0PauseTrigger()
}

func (db *DB) pauseCompaction(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	case <-db.closeC:
		db.compactionExitTransact()
	}
}

type cCmd interface {
	ack(err error)
}

type cAuto struct {
	// Note for table compaction, an non-empty ackC represents it's a compaction waiting command.
	ackC chan<- error
}

func (r cAuto) ack(err error) {
	if r.ackC != nil {
		defer func() {
			_ = recover()
		}()
		r.ackC <- err
	}
}

type cRange struct {
	level    int
	min, max []byte
	ackC     chan<- error
}

func (r cRange) ack(err error) {
	if r.ackC != nil {
		defer func() {
			_ = recover()
		}()
		r.ackC <- err
	}
}

// This will trigger auto compaction but will not wait for it.
func (db *DB) compTrigger(compC chan<- cCmd) {
	select {
	case compC <- cAuto{}:
	default:
	}
}

// This will trigger auto compaction and/or wait for all compaction to be done.
// 阻塞式进行一次minor compaction
func (db *DB) compTriggerWait(compC chan<- cCmd) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cAuto{ch}:
	case err = <-db.compErrC:
		return
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

// Send range compaction request.
func (db *DB) compTriggerRange(compC chan<- cCmd, level int, min, max []byte) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cRange{level, min, max, ch}:
	case err := <-db.compErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

func (db *DB) mCompaction() {
	var x cCmd

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		select {
		case x = <-db.mcompCmdC:
			switch x.(type) {
			case cAuto:
				db.memCompaction()
				x.ack(nil)
				x = nil // todo 为什么置为nil?
			default:
				panic("leveldb: unknown command")
			}
		case <-db.closeC:
			return
		}
	}
}

func (db *DB) tCompaction() {
	var (
		x     cCmd
		waitQ []cCmd
	)

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		for i := range waitQ {
			waitQ[i].ack(ErrClosed)
			waitQ[i] = nil
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		// 首先有积分大于1的需要进行table compaction
		if db.tableNeedCompaction() {
			select {
			// 然后有地方触发table compaction
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			default:
			}
			// Resume write operation as soon as possible.
			// 恢复因为GetWriteL0PauseTrigger（12）触发的写阻塞
			if len(waitQ) > 0 && db.resumeWrite() {
				for i := range waitQ {
					waitQ[i].ack(nil)
					waitQ[i] = nil
				}
				waitQ = waitQ[:0]
			}
		} else {
			for i := range waitQ {
				waitQ[i].ack(nil)
				waitQ[i] = nil
			}
			waitQ = waitQ[:0]
			select {
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			}
		}
		// 开始进行table compaction
		if x != nil {
			switch cmd := x.(type) {
			case cAuto:
				if cmd.ackC != nil {
					// Check the write pause state before caching it.
					if db.resumeWrite() {
						x.ack(nil)
					} else {
						waitQ = append(waitQ, x)
					}
				}
			case cRange:
				x.ack(db.tableRangeCompaction(cmd.level, cmd.min, cmd.max))
			default:
				panic("leveldb: unknown command")
			}
			x = nil
		}
		db.tableAutoCompaction()
	}
}

// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"

	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func sharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

type blockWriter struct {
	restartInterval int
	buf             util.Buffer
	nEntries        int
	prevKey         []byte   // 缓存index或data block中每次写入的key
	restarts        []uint32 // 记录每个restart在buf中的偏移量
	scratch         []byte
}

// 写入kv到buf，并每间隔16个写入一个restart
// index 或者 data 的block
func (w *blockWriter) append(key, value []byte) (err error) {
	nShared := 0
	// data block中每16个会重新设置一个restart
	// index block中每2个会重新设置一个restart
	if w.nEntries%w.restartInterval == 0 {
		w.restarts = append(w.restarts, uint32(w.buf.Len()))
	} else {
		nShared = sharedPrefixLen(w.prevKey, key)
	}
	n := binary.PutUvarint(w.scratch[0:], uint64(nShared))
	n += binary.PutUvarint(w.scratch[n:], uint64(len(key)-nShared))
	n += binary.PutUvarint(w.scratch[n:], uint64(len(value)))
	if _, err = w.buf.Write(w.scratch[:n]); err != nil {
		return err
	}
	if _, err = w.buf.Write(key[nShared:]); err != nil {
		return err
	}
	if _, err = w.buf.Write(value); err != nil {
		return err
	}
	// 记录前一个写入的key，并不是每个restart的第一个key
	// 这里的key是完整的key
	// 对于data block写入的是完整的ikey
	// 对于index block写入的是完整的separator
	w.prevKey = append(w.prevKey[:0], key...)
	w.nEntries++
	return nil
}

// 把每个restart的索引和restart的数量写入buf
func (w *blockWriter) finish() error {
	// Write restarts entry.
	if w.nEntries == 0 {
		// Must have at least one restart entry.
		w.restarts = append(w.restarts, 0)
	}
	// 追加restarts的数量，只有在finish时候才会调用
	w.restarts = append(w.restarts, uint32(len(w.restarts)))
	for _, x := range w.restarts {
		buf4 := w.buf.Alloc(4)
		binary.LittleEndian.PutUint32(buf4, x)
	}
	return nil
}

func (w *blockWriter) reset() {
	w.buf.Reset()
	w.nEntries = 0
	w.restarts = w.restarts[:0]
}

func (w *blockWriter) bytesLen() int {
	restartsLen := len(w.restarts)
	if restartsLen == 0 {
		restartsLen = 1
	}
	return w.buf.Len() + 4*restartsLen + 4
}

type filterWriter struct {
	generator filter.FilterGenerator
	buf       util.Buffer
	nKeys     int      // key的个数
	offsets   []uint32 // 每个filter block对sst中每2KB数据的偏移量，一个filter对应着2KB的数据，这里的索引是被包含范围的索引，也就是截止位置的索引
	baseLg    uint
}

func (w *filterWriter) add(key []byte) {
	if w.generator == nil {
		return
	}
	w.generator.Add(key)
	w.nKeys++
}

func (w *filterWriter) flush(offset uint64) {
	if w.generator == nil {
		return
	}

	// offset / 2048，  block data 默认4KB，所以这里默认每2KB的数据构造一个filter
	for x := int(offset / uint64(1<<w.baseLg)); x > len(w.offsets); {
		w.generate()
	}
}

func (w *filterWriter) finish() error {
	if w.generator == nil {
		return nil
	}
	// Generate last keys.

	if w.nKeys > 0 {
		w.generate()
	}
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	for _, x := range w.offsets {
		buf4 := w.buf.Alloc(4)
		binary.LittleEndian.PutUint32(buf4, x)
	}
	return w.buf.WriteByte(byte(w.baseLg))
}

func (w *filterWriter) generate() {
	// Record offset.
	// data block中的
	w.offsets = append(w.offsets, uint32(w.buf.Len()))
	// Generate filters.
	if w.nKeys > 0 {
		w.generator.Generate(&w.buf)
		w.nKeys = 0
	}
}

// Writer is a table writer.
type Writer struct {
	writer io.Writer
	err    error
	// Options
	cmp         comparer.Comparer
	filter      filter.Filter
	compression opt.Compression
	blockSize   int

	bpool       *util.BufferPool
	dataBlock   blockWriter
	indexBlock  blockWriter
	filterBlock filterWriter
	pendingBH   blockHandle // 上一个写入的data block的索引
	offset      uint64      // dataBlock的当前最新索引
	nEntries    int
	// Scratch allocated enough for 5 uvarint. Block writer should not use
	// first 20-bytes since it will be used to encode block handle, which
	// then passed to the block writer itself.
	scratch            [50]byte
	comparerScratch    []byte
	compressionScratch []byte // 防压缩后的数据
}

// 写入一个data block，并返回这个data block对应的索引bh
// 压缩数据并追加压缩算法和c2c值
// 然后将数据写入数据流中，并更新文件索引，返回写入流的data索引
func (w *Writer) writeBlock(buf *util.Buffer, compression opt.Compression) (bh blockHandle, err error) {
	// Compress the buffer if necessary.
	var b []byte
	if compression == opt.SnappyCompression {
		// Allocate scratch enough for compression and block trailer.
		// n 是压缩后的数据size+5个字节，5个字节是数据压缩类型和数据的c2c值
		if n := snappy.MaxEncodedLen(buf.Len()) + blockTrailerLen; len(w.compressionScratch) < n {
			w.compressionScratch = make([]byte, n)
		}
		// 压缩后的data
		// 这里是耗费cpu的地方
		compressed := snappy.Encode(w.compressionScratch, buf.Bytes())
		n := len(compressed)
		b = compressed[:n+blockTrailerLen]
		b[n] = blockTypeSnappyCompression // 写入数据压缩类型
	} else {
		tmp := buf.Alloc(blockTrailerLen)
		tmp[0] = blockTypeNoCompression
		b = buf.Bytes()
	}

	// Calculate the checksum.
	// 计算并写入c2c
	n := len(b) - 4
	checksum := util.NewCRC(b[:n]).Value()
	binary.LittleEndian.PutUint32(b[n:], checksum)

	// Write the buffer to the file.
	// data写入底层数据流
	_, err = w.writer.Write(b)
	if err != nil {
		return
	}
	// 写入流的data的起始偏移量
	bh = blockHandle{w.offset, uint64(len(b) - blockTrailerLen)}
	// 偏移量更新
	w.offset += uint64(len(b))
	return
}

// 把上一个data block的索引写入index block
// 然后再将dataBlock中的preKey，sst的pendingBH置为空
func (w *Writer) flushPendingBH(key []byte) error {
	// 一个data block中会写入多条entry，data block没写入file的时候，这里一直为0，从这里直接退出
	if w.pendingBH.length == 0 {
		return nil
	}
	// 检索出来跟完整的前置pre key重叠的部分，但是重叠的最后一个byte为什么+1？
	var separator []byte
	if len(key) == 0 {
		separator = w.cmp.Successor(w.comparerScratch[:0], w.dataBlock.prevKey)
	} else {
		// 这里的Separator方法需要深入理解
		// 逻辑是上一个（也就是正在被构建index的）data block中按序最后一个key（key key）和下一个data block中的第一个key（key）计算出来一个separator
		// 然后把这个separator=>bh作为index block中的一条记录，表示这个data block中的key都小于这个separator.
		// separator的计算方法是，采用完整的key，pre key和key，计算出来pre key小于等于key的部分，得到公共的部分dst,然后再把dst最后一个字符+1。
		// 例子：dock, duck => e
		separator = w.cmp.Separator(w.comparerScratch[:0], w.dataBlock.prevKey, key)
	}
	if separator == nil {
		separator = w.dataBlock.prevKey
	} else {
		w.comparerScratch = separator
	}
	// 上一个data block的偏移量
	n := encodeBlockHandle(w.scratch[:20], w.pendingBH)
	// Append the block handle to the index block.
	// 把上一个data block的索引，写入index block
	// 这里的key是separator
	if err := w.indexBlock.append(separator, w.scratch[:n]); err != nil {
		return err
	}
	// Reset prev key of the data block.
	w.dataBlock.prevKey = w.dataBlock.prevKey[:0]
	// Clear pending block handle.
	w.pendingBH = blockHandle{}
	return nil
}

// 对于data block 先把每个restart的索引和restart的数量写入buf，再构建data block
// 然后构建filter block，也在buf中
func (w *Writer) finishBlock() error {
	// 把每个restart的索引和restart的数量写入buf
	if err := w.dataBlock.finish(); err != nil {
		return err
	}
	// 写入一个data block，并返回这个data block对应的索引bh
	// 压缩数据并追加压缩算法和c2c值，这里的数据包括entry,restart索引和restart数量
	// 然后将数据写入流，并更新文件索引，返回写入流的data索引
	bh, err := w.writeBlock(&w.dataBlock.buf, w.compression)
	if err != nil {
		return err
	}
	// 写入的data block的索引
	w.pendingBH = bh
	// Reset the data block.
	// data block写入后就重置
	w.dataBlock.reset()
	// Flush the filter block.
	// 构建filter block，也在buf中
	w.filterBlock.flush(w.offset)
	return nil
}

// Append appends key/value pair to the table. The keys passed must
// be in increasing order.
//
// It is safe to modify the contents of the arguments after Append returns.
// 把上一个data block的索引写入index block
// 写kv到data block的buf中
// 写key到布隆过滤器的keyHashes中，等每间隔2k数据时，再实际写入布隆过滤器
func (w *Writer) Append(key, value []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.nEntries > 0 && w.cmp.Compare(w.dataBlock.prevKey, key) >= 0 {
		w.err = fmt.Errorf("leveldb/table: Writer: keys are not in increasing order: %q, %q", w.dataBlock.prevKey, key)
		return w.err
	}

	// 把上一个data block的索引写入index block
	// 然后再将dataBlock中的preKey，sst的pendingBH置为空
	if err := w.flushPendingBH(key); err != nil {
		return err
	}
	// Append key/value pair to the data block.
	// 写kv到data block的buf中
	if err := w.dataBlock.append(key, value); err != nil {
		return err
	}
	// Add key to the filter block.
	// 写key到布隆过滤器的keyHashes中，等每间隔2k数据时，再实际写入布隆过滤器
	w.filterBlock.add(key)

	// Finish the data block if block size target reached.
	// data block够4KB了
	// 对于data block 先把每个restart的索引和restart的数量写入buf，再构建data block
	// 然后构建filter block，也是在buf中
	if w.dataBlock.bytesLen() >= w.blockSize {
		if err := w.finishBlock(); err != nil {
			w.err = err
			return w.err
		}
	}
	w.nEntries++
	return nil
}

// BlocksLen returns number of blocks written so far.
func (w *Writer) BlocksLen() int {
	n := w.indexBlock.nEntries
	if w.pendingBH.length > 0 {
		// Includes the pending block.
		n++
	}
	return n
}

// EntriesLen returns number of entries added so far.
func (w *Writer) EntriesLen() int {
	return w.nEntries
}

// BytesLen returns number of bytes written so far.
func (w *Writer) BytesLen() int {
	return int(w.offset)
}

// Close will finalize the table. Calling Append is not possible
// after Close, but calling BlocksLen, EntriesLen and BytesLen
// is still possible.
func (w *Writer) Close() error {
	defer func() {
		if w.bpool != nil {
			// Buffer.Bytes() returns [offset:] of the buffer.
			// We need to Reset() so that the offset = 0, resulting
			// in buf.Bytes() returning the whole allocated bytes.
			w.dataBlock.buf.Reset()
			w.bpool.Put(w.dataBlock.buf.Bytes())
		}
	}()

	if w.err != nil {
		return w.err
	}

	// Write the last data block. Or empty data block if there
	// aren't any data blocks at all.
	if w.dataBlock.nEntries > 0 || w.nEntries == 0 {
		if err := w.finishBlock(); err != nil {
			w.err = err
			return w.err
		}
	}
	if err := w.flushPendingBH(nil); err != nil {
		return err
	}

	// Write the filter block.
	var filterBH blockHandle
	if err := w.filterBlock.finish(); err != nil {
		return err
	}
	if buf := &w.filterBlock.buf; buf.Len() > 0 {
		filterBH, w.err = w.writeBlock(buf, opt.NoCompression)
		if w.err != nil {
			return w.err
		}
	}

	// Write the metaindex block.
	if filterBH.length > 0 {
		key := []byte("filter." + w.filter.Name())
		n := encodeBlockHandle(w.scratch[:20], filterBH)
		if err := w.dataBlock.append(key, w.scratch[:n]); err != nil {
			return err
		}
	}
	if err := w.dataBlock.finish(); err != nil {
		return err
	}
	metaindexBH, err := w.writeBlock(&w.dataBlock.buf, w.compression)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the index block.
	if err := w.indexBlock.finish(); err != nil {
		return err
	}
	indexBH, err := w.writeBlock(&w.indexBlock.buf, w.compression)
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the table footer.
	footer := w.scratch[:footerLen]
	for i := range footer {
		footer[i] = 0
	}
	n := encodeBlockHandle(footer, metaindexBH)
	encodeBlockHandle(footer[n:], indexBH)
	copy(footer[footerLen-len(magic):], magic)
	if _, err := w.writer.Write(footer); err != nil {
		w.err = err
		return w.err
	}
	w.offset += footerLen

	w.err = errors.New("leveldb/table: writer is closed")
	return nil
}

// NewWriter creates a new initialized table writer for the file.
//
// Table writer is not safe for concurrent use.
func NewWriter(f io.Writer, o *opt.Options, pool *util.BufferPool, size int) *Writer {
	var bufBytes []byte
	if pool == nil {
		bufBytes = make([]byte, size)
	} else {
		bufBytes = pool.Get(size)
	}
	bufBytes = bufBytes[:0]

	w := &Writer{
		writer:          f,
		cmp:             o.GetComparer(),
		filter:          o.GetFilter(),
		compression:     o.GetCompression(),
		blockSize:       o.GetBlockSize(),
		comparerScratch: make([]byte, 0),
		bpool:           pool,
		dataBlock:       blockWriter{buf: *util.NewBuffer(bufBytes)},
	}
	// data block
	w.dataBlock.restartInterval = o.GetBlockRestartInterval()
	// The first 20-bytes are used for encoding block handle.
	w.dataBlock.scratch = w.scratch[20:]
	// index block
	w.indexBlock.restartInterval = 1
	w.indexBlock.scratch = w.scratch[20:]
	// filter block
	if w.filter != nil {
		w.filterBlock.generator = w.filter.NewGenerator()
		w.filterBlock.baseLg = uint(o.GetFilterBaseLg())
		w.filterBlock.flush(0)
	}
	return w
}

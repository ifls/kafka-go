package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

type writeBuffer struct {
	w io.Writer
	b [16]byte
}

func (wb *writeBuffer) writeInt8(i int8) {
	wb.b[0] = byte(i)
	wb.Write(wb.b[:1])
}

func (wb *writeBuffer) writeInt16(i int16) {
	binary.BigEndian.PutUint16(wb.b[:2], uint16(i))
	wb.Write(wb.b[:2])
}

func (wb *writeBuffer) writeInt32(i int32) {
	binary.BigEndian.PutUint32(wb.b[:4], uint32(i))
	wb.Write(wb.b[:4])
}

func (wb *writeBuffer) writeInt64(i int64) {
	binary.BigEndian.PutUint64(wb.b[:8], uint64(i))
	wb.Write(wb.b[:8])
}

func (wb *writeBuffer) writeVarInt(i int64) {
	u := uint64((i << 1) ^ (i >> 63))
	n := 0

	for u >= 0x80 && n < len(wb.b) {
		wb.b[n] = byte(u) | 0x80
		u >>= 7
		n++
	}

	if n < len(wb.b) {
		wb.b[n] = byte(u)
		n++
	}

	wb.Write(wb.b[:n])
}

func (wb *writeBuffer) writeString(s string) {
	wb.writeInt16(int16(len(s)))
	wb.WriteString(s)
}

func (wb *writeBuffer) writeVarString(s string) {
	wb.writeVarInt(int64(len(s)))
	wb.WriteString(s)
}

func (wb *writeBuffer) writeNullableString(s *string) {
	if s == nil {
		wb.writeInt16(-1)
	} else {
		wb.writeString(*s)
	}
}

func (wb *writeBuffer) writeBytes(b []byte) {
	n := len(b)
	if b == nil {
		n = -1
	}
	wb.writeInt32(int32(n))
	wb.Write(b)
}

func (wb *writeBuffer) writeVarBytes(b []byte) {
	if b != nil {
		wb.writeVarInt(int64(len(b)))
		wb.Write(b)
	} else {
		//-1 is used to indicate nil key
		wb.writeVarInt(-1)
	}
}

func (wb *writeBuffer) writeBool(b bool) {
	v := int8(0)
	if b {
		v = 1
	}
	wb.writeInt8(v)
}

func (wb *writeBuffer) writeArrayLen(n int) {
	wb.writeInt32(int32(n))
}

func (wb *writeBuffer) writeArray(n int, f func(int)) {
	wb.writeArrayLen(n)
	for i := 0; i < n; i++ {
		f(i)
	}
}

func (wb *writeBuffer) writeVarArray(n int, f func(int)) {
	wb.writeVarInt(int64(n))
	for i := 0; i < n; i++ {
		f(i)
	}
}

func (wb *writeBuffer) writeStringArray(a []string) {
	wb.writeArray(len(a), func(i int) { wb.writeString(a[i]) })
}

func (wb *writeBuffer) writeInt32Array(a []int32) {
	wb.writeArray(len(a), func(i int) { wb.writeInt32(a[i]) })
}

func (wb *writeBuffer) write(a interface{}) {
	switch v := a.(type) {
	case int8:
		wb.writeInt8(v)
	case int16:
		wb.writeInt16(v)
	case int32:
		wb.writeInt32(v)
	case int64:
		wb.writeInt64(v)
	case string:
		wb.writeString(v)
	case []byte:
		wb.writeBytes(v)
	case bool:
		wb.writeBool(v)
	case writable:
		v.writeTo(wb)
	default:
		panic(fmt.Sprintf("unsupported type: %T", a))
	}
}

func (wb *writeBuffer) Write(b []byte) (int, error) {
	return wb.w.Write(b)
}

func (wb *writeBuffer) WriteString(s string) (int, error) {
	return io.WriteString(wb.w, s)
}

func (wb *writeBuffer) Flush() error {
	if x, ok := wb.w.(interface{ Flush() error }); ok {
		return x.Flush()
	}
	return nil
}

type writable interface {
	writeTo(*writeBuffer)
}

func (wb *writeBuffer) writeFetchRequestV2(correlationID int32, clientID, topic string, partition int32, offset int64, minBytes, maxBytes int, maxWait time.Duration) error {
	h := requestHeader{
		ApiKey:        int16(fetch),
		ApiVersion:    int16(v2),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		4 + // replica ID
		4 + // max wait time
		4 + // min bytes
		4 + // topic array length
		sizeofString(topic) +
		4 + // partition array length
		4 + // partition
		8 + // offset
		4 // max bytes

	h.writeTo(wb)
	wb.writeInt32(-1) // replica ID
	wb.writeInt32(milliseconds(maxWait))
	wb.writeInt32(int32(minBytes))

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)
	wb.writeInt64(offset)
	wb.writeInt32(int32(maxBytes))

	return wb.Flush()
}

func (wb *writeBuffer) writeFetchRequestV5(correlationID int32, clientID, topic string, partition int32, offset int64, minBytes, maxBytes int, maxWait time.Duration, isolationLevel int8) error {
	h := requestHeader{
		ApiKey:        int16(fetch),
		ApiVersion:    int16(v5),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		4 + // replica ID
		4 + // max wait time
		4 + // min bytes
		4 + // max bytes
		1 + // isolation level
		4 + // topic array length
		sizeofString(topic) +
		4 + // partition array length
		4 + // partition
		8 + // offset
		8 + // log start offset
		4 // max bytes

	h.writeTo(wb)
	wb.writeInt32(-1) // replica ID
	wb.writeInt32(milliseconds(maxWait))
	wb.writeInt32(int32(minBytes))
	wb.writeInt32(int32(maxBytes))
	wb.writeInt8(isolationLevel) // isolation level 0 - read uncommitted

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)
	wb.writeInt64(offset)
	wb.writeInt64(int64(0)) // log start offset only used when is sent by follower
	wb.writeInt32(int32(maxBytes))

	return wb.Flush()
}

func (wb *writeBuffer) writeFetchRequestV10(correlationID int32, clientID, topic string, partition int32, offset int64, minBytes, maxBytes int, maxWait time.Duration, isolationLevel int8) error {
	h := requestHeader{
		ApiKey:        int16(fetch),
		ApiVersion:    int16(v10),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		4 + // replica ID
		4 + // max wait time
		4 + // min bytes
		4 + // max bytes
		1 + // isolation level
		4 + // session ID
		4 + // session epoch
		4 + // topic array length
		sizeofString(topic) +
		4 + // partition array length
		4 + // partition
		4 + // current leader epoch
		8 + // fetch offset
		8 + // log start offset
		4 + // partition max bytes
		4 // forgotten topics data

	h.writeTo(wb)                        // 写入头部
	wb.writeInt32(-1)                    // 4B 消费者不用填， 从broker 向主broker 获取时才要填 // replica ID
	wb.writeInt32(milliseconds(maxWait)) // 4B 超时
	wb.writeInt32(int32(minBytes))       // 4B 响应里 最小的累积字节数
	wb.writeInt32(int32(maxBytes))       // 4B 最大字节数
	wb.writeInt8(isolationLevel)         // 1B 接口入参中配置 // isolation level 0 - read uncommitted
	wb.writeInt32(0)                     // 4B fetch session id     //FIXME
	wb.writeInt32(-1)                    // 4B session epoch, 用于在一个session里顺序化请求     //FIXME

	// topic array
	wb.writeArrayLen(1)   // 4B 只发一个topic的
	wb.writeString(topic) // 变长的string，topic name

	// partition array
	// partitions
	wb.writeArrayLen(1)            // 4B 只发到一个partition
	wb.writeInt32(partition)       // 4B partition index
	wb.writeInt32(-1)              // 4B current_leader_epoch 此partition的当前leader epoch //FIXME
	wb.writeInt64(offset)          // 4B fetch_offset
	wb.writeInt64(int64(0))        // 4B log_start_offset // log start offset only used when is sent by follower
	wb.writeInt32(int32(maxBytes)) // 4B partition_max_bytes 从此partition拿出的最大字节数，因为只fetch一个topic下的一个partition， 所以和上面的maxBytes的值时一样的

	// forgotten topics array
	// forgetten_topics_data 用于 增量fetch请求(这时partitions不需要，)
	wb.writeArrayLen(0) // forgotten topics not supported yet

	return wb.Flush()
}

func (wb *writeBuffer) writeListOffsetRequestV1(correlationID int32, clientID, topic string, partition int32, time int64) error {
	h := requestHeader{
		ApiKey:        int16(listOffsets),
		ApiVersion:    int16(v1),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		4 + // replica ID
		4 + // topic array length
		sizeofString(topic) + // topic
		4 + // partition array length
		4 + // partition
		8 // time

	h.writeTo(wb)
	wb.writeInt32(-1) // replica ID

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)
	wb.writeInt64(time)

	return wb.Flush()
}

func (wb *writeBuffer) writeProduceRequestV2(codec CompressionCodec, correlationID int32, clientID, topic string, partition int32, timeout time.Duration, requiredAcks int16, msgs ...Message) (err error) {
	var size int32
	var attributes int8
	var compressed *bytes.Buffer

	if codec == nil {
		size = messageSetSize(msgs...)
	} else {
		compressed, attributes, size, err = compressMessageSet(codec, msgs...)
		if err != nil {
			return
		}
		msgs = []Message{{Value: compressed.Bytes()}}
	}

	h := requestHeader{
		ApiKey:        int16(produce),
		ApiVersion:    int16(v2),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		2 + // required acks
		4 + // timeout
		4 + // topic array length
		sizeofString(topic) + // topic
		4 + // partition array length
		4 + // partition
		4 + // message set size
		size

	h.writeTo(wb)
	wb.writeInt16(requiredAcks) // required acks
	wb.writeInt32(milliseconds(timeout))

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)

	wb.writeInt32(size)
	cw := &crc32Writer{table: crc32.IEEETable}

	for _, msg := range msgs {
		wb.writeMessage(msg.Offset, attributes, msg.Time, msg.Key, msg.Value, cw)
	}

	releaseBuffer(compressed)
	return wb.Flush()
}

func (wb *writeBuffer) writeProduceRequestV3(correlationID int32, clientID, topic string, partition int32, timeout time.Duration, requiredAcks int16, transactionalID *string, recordBatch *recordBatch) (err error) {

	h := requestHeader{
		ApiKey:        int16(produce),
		ApiVersion:    int16(v3),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}

	h.Size = (h.size() - 4) +
		sizeofNullableString(transactionalID) +
		2 + // required acks
		4 + // timeout
		4 + // topic array length
		sizeofString(topic) + // topic
		4 + // partition array length
		4 + // partition
		4 + // message set size
		recordBatch.size

	h.writeTo(wb)
	wb.writeNullableString(transactionalID)
	wb.writeInt16(requiredAcks) // required acks
	wb.writeInt32(milliseconds(timeout))

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)

	recordBatch.writeTo(wb)

	return wb.Flush()
}

// 发送数据的 函数
func (wb *writeBuffer) writeProduceRequestV7(correlationID int32, clientID, topic string, partition int32, timeout time.Duration, requiredAcks int16, transactionalID *string, recordBatch *recordBatch) (err error) {

	h := requestHeader{
		ApiKey:        int16(produce),
		ApiVersion:    int16(v7),
		CorrelationID: correlationID,
		ClientID:      clientID,
	}
	h.Size = (h.size() - 4) +
		sizeofNullableString(transactionalID) +
		2 + // required acks
		4 + // timeout
		4 + // topic array length
		sizeofString(topic) + // topic
		4 + // partition array length
		4 + // partition
		4 + // message set size
		recordBatch.size

	h.writeTo(wb)

	// v7 -> transactional_id acks timeout_ms [topic_data]
	wb.writeNullableString(transactionalID)
	wb.writeInt16(requiredAcks) // required acks
	wb.writeInt32(milliseconds(timeout))

	// topic array
	wb.writeArrayLen(1)
	wb.writeString(topic)

	// partition array
	wb.writeArrayLen(1)
	wb.writeInt32(partition)

	// 写入 RECORDS
	recordBatch.writeTo(wb)

	return wb.Flush()
}

func (wb *writeBuffer) writeRecordBatch(attributes int16, size int32, count int, baseTime, lastTime time.Time, write func(*writeBuffer)) {
	var (
		baseTimestamp   = timestamp(baseTime)
		lastTimestamp   = timestamp(lastTime)
		lastOffsetDelta = int32(count - 1)
		producerID      = int64(-1)    // default producer id for now
		producerEpoch   = int16(-1)    // default producer epoch for now
		baseSequence    = int32(-1)    // default base sequence
		recordCount     = int32(count) // record count
		writerBackup    = wb.w
	)

	// dry run to compute the checksum
	cw := &crc32Writer{table: crc32.MakeTable(crc32.Castagnoli)}
	wb.w = cw
	cw.writeInt16(attributes) // attributes, timestamp type 0 - create time, not part of a transaction, no control messages
	cw.writeInt32(lastOffsetDelta)
	cw.writeInt64(baseTimestamp)
	cw.writeInt64(lastTimestamp)
	cw.writeInt64(producerID)
	cw.writeInt16(producerEpoch)
	cw.writeInt32(baseSequence)
	cw.writeInt32(recordCount)
	write(wb)
	wb.w = writerBackup

	// actual write to the output buffer
	wb.writeInt64(int64(0))         // baseOffset 8B
	wb.writeInt32(int32(size - 12)) // batchLength 4B // 12 = batch length + base offset sizes
	wb.writeInt32(-1)               // partitionLeaderEpoch 4B   // partition leader epoch
	wb.writeInt8(2)                 // 魔数 1B 当前固定是2   // magic byte
	wb.writeInt32(int32(cw.crc32))  // crc32校验码 4B

	// 			bit 0~2:
	//				0: no compression
	//				1: gzip
	//				2: snappy
	//				3: lz4
	//				4: zstd
	//			bit 3: timestampType
	//			bit 4: isTransactional (0 means not transactional)
	//			bit 5: isControlBatch (0 means not a control batch)
	//			bit 6~15: unused
	wb.writeInt16(attributes)      // 2B, 值的含义如上
	wb.writeInt32(lastOffsetDelta) // lastOffsetDelta 4B
	wb.writeInt64(baseTimestamp)   // firstTimestamp 8B
	wb.writeInt64(lastTimestamp)   // maxTimestamp 8B
	wb.writeInt64(producerID)      // producerId 4B
	wb.writeInt16(producerEpoch)   // producerEpoch 2B
	wb.writeInt32(baseSequence)    // baseSequence 4B
	wb.writeInt32(recordCount)     // RecordsLength
	write(wb)
}

func compressMessageSet(codec CompressionCodec, msgs ...Message) (compressed *bytes.Buffer, attributes int8, size int32, err error) {
	compressed = acquireBuffer()
	compressor := codec.NewWriter(compressed)
	wb := &writeBuffer{w: compressor}
	cw := &crc32Writer{table: crc32.IEEETable}

	for offset, msg := range msgs {
		wb.writeMessage(int64(offset), 0, msg.Time, msg.Key, msg.Value, cw)
	}

	if err = compressor.Close(); err != nil {
		releaseBuffer(compressed)
		return
	}

	attributes = codec.Code()
	size = messageSetSize(Message{Value: compressed.Bytes()})
	return
}

func (wb *writeBuffer) writeMessage(offset int64, attributes int8, time time.Time, key, value []byte, cw *crc32Writer) {
	const magicByte = 1 // compatible with kafka 0.10.0.0+

	timestamp := timestamp(time)
	size := messageSize(key, value)

	// dry run to compute the checksum
	cw.crc32 = 0
	cw.writeInt8(magicByte)
	cw.writeInt8(attributes)
	cw.writeInt64(timestamp)
	cw.writeBytes(key)
	cw.writeBytes(value)

	// actual write to the output buffer
	wb.writeInt64(offset)
	wb.writeInt32(size)
	wb.writeInt32(int32(cw.crc32))
	wb.writeInt8(magicByte)
	wb.writeInt8(attributes)
	wb.writeInt64(timestamp)
	wb.writeBytes(key)
	wb.writeBytes(value)
}

// Messages with magic >2 are called records. This method writes messages using message format 2.
func (wb *writeBuffer) writeRecord(attributes int8, baseTime time.Time, offset int64, msg Message) {
	timestampDelta := msg.Time.Sub(baseTime)
	offsetDelta := int64(offset)

	// varint 序列化的格式 和protobuf一样
	wb.writeVarInt(int64(recordSize(&msg, timestampDelta, offsetDelta))) // length varint
	wb.writeInt8(attributes)                                             // 		attributes: int8 bit 0~7: unused
	wb.writeVarInt(int64(milliseconds(timestampDelta)))                  // timestampDelta varlong
	wb.writeVarInt(offsetDelta)                                          // offsetDelta varint

	wb.writeVarBytes(msg.Key)   // keyLength varint(-1表示没有) key []byte
	wb.writeVarBytes(msg.Value) // valueLength varint(-1表示没有) value []byte

	// 写入head
	wb.writeVarArray(len(msg.Headers), func(i int) { // headerLength varint
		h := &msg.Headers[i]
		wb.writeVarString(h.Key)  // headerKeyLength: varint headerKey: String
		wb.writeVarBytes(h.Value) // headerValueLength: varint Value: byte[]
	})
}

func varIntLen(i int64) int {
	u := uint64((i << 1) ^ (i >> 63)) // zig-zag encoding
	n := 0

	for u >= 0x80 {
		u >>= 7
		n++
	}

	return n + 1
}

func varBytesLen(b []byte) int {
	return varIntLen(int64(len(b))) + len(b)
}

func varStringLen(s string) int {
	return varIntLen(int64(len(s))) + len(s)
}

func varArrayLen(n int, f func(int) int) int {
	size := varIntLen(int64(n))
	for i := 0; i < n; i++ {
		size += f(i)
	}
	return size
}

func messageSize(key, value []byte) int32 {
	return 4 + // crc
		1 + // magic byte
		1 + // attributes
		8 + // timestamp
		sizeofBytes(key) +
		sizeofBytes(value)
}

func messageSetSize(msgs ...Message) (size int32) {
	for _, msg := range msgs {
		size += 8 + // offset
			4 + // message size
			4 + // crc
			1 + // magic byte
			1 + // attributes
			8 + // timestamp
			sizeofBytes(msg.Key) +
			sizeofBytes(msg.Value)
	}
	return
}

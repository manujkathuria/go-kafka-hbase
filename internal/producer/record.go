package producer

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Magic byte identifying this record format version.
const RecordMagic byte = 0x01

// Fixed payload size in bytes (128 bytes).
const FixedPayloadSize = 128

// Header size:
// Magic: 1 byte
// RunID: 16 bytes (UUID / raw bytes)
// SeqNum: 8 bytes (uint64 big-endian)
// PayloadSize: 2 bytes (uint16 big-endian)
// Total header size: 27 bytes.
const RecordHeaderSize = 1 + 16 + 8 + 2

// Number of key buckets for partition distribution.
const NumKeyBuckets = 16

var (
	ErrPayloadTooLarge = errors.New("producer: record payload exceeds fixed record capacity")
	ErrBufferTooSmall  = errors.New("producer: destination buffer too small for record")
	ErrInvalidMagic    = errors.New("producer: invalid record magic byte")
	ErrTruncatedRecord = errors.New("producer: record data truncated")
)

// SyntheticRecord represents a generated message fixture.
type SyntheticRecord struct {
	RunID    [16]byte
	SeqNum   uint64
	Payload  [FixedPayloadSize]byte
	ValidLen uint16 // actual length of valid content inside Payload
}

// KeyForSeq returns a deterministic key byte slice for a given sequence number.
// Keys are formatted as "key-<bucket>" where bucket = seq % NumKeyBuckets.
// This distributes traffic across Kafka partitions deterministically while keeping key mapping simple.
func KeyForSeq(seq uint64) []byte {
	bucket := seq % NumKeyBuckets
	return fmt.Appendf(nil, "key-%02d", bucket)
}

// Encode writes the synthetic record into dst in binary format:
// [1 byte magic][16 bytes run ID][8 bytes seq uint64][2 bytes valid len uint16][valid payload bytes padded with 0 to FixedPayloadSize]
// The total encoded size is RecordHeaderSize + FixedPayloadSize.
func (r *SyntheticRecord) Encode(dst []byte) (int, error) {
	totalLen := RecordHeaderSize + FixedPayloadSize
	if len(dst) < totalLen {
		return 0, ErrBufferTooSmall
	}
	if int(r.ValidLen) > FixedPayloadSize {
		return 0, ErrPayloadTooLarge
	}

	dst[0] = RecordMagic
	copy(dst[1:17], r.RunID[:])
	binary.BigEndian.PutUint64(dst[17:25], r.SeqNum)
	binary.BigEndian.PutUint16(dst[25:27], r.ValidLen)
	copy(dst[27:totalLen], r.Payload[:])

	return totalLen, nil
}

// Decode parses raw bytes into the SyntheticRecord struct.
func (r *SyntheticRecord) Decode(src []byte) error {
	totalLen := RecordHeaderSize + FixedPayloadSize
	if len(src) < totalLen {
		return ErrTruncatedRecord
	}
	if src[0] != RecordMagic {
		return fmt.Errorf("%w: got 0x%02x, want 0x%02x", ErrInvalidMagic, src[0], RecordMagic)
	}

	copy(r.RunID[:], src[1:17])
	r.SeqNum = binary.BigEndian.Uint64(src[17:25])
	r.ValidLen = binary.BigEndian.Uint16(src[25:27])
	if int(r.ValidLen) > FixedPayloadSize {
		return ErrPayloadTooLarge
	}
	copy(r.Payload[:], src[27:totalLen])

	return nil
}

// FillDeterministicPayload populates the record's payload deterministically
// with a pattern based on RunID and SeqNum.
func (r *SyntheticRecord) FillDeterministicPayload(runID [16]byte, seq uint64) {
	r.RunID = runID
	r.SeqNum = seq
	r.ValidLen = FixedPayloadSize

	// Fill with a recognizable pattern:
	// prefix "RUN:<hex-prefix>:SEQ:<seq>:" followed by repeating deterministic bytes
	prefix := fmt.Sprintf("RUN:%x:SEQ:%d:", runID[:4], seq)
	copied := copy(r.Payload[:], prefix)
	for i := copied; i < FixedPayloadSize; i++ {
		r.Payload[i] = byte((uint64(i) + seq) & 0xff)
	}
}

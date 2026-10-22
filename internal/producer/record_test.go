package producer

import (
	"bytes"
	"errors"
	"testing"
)

func TestSyntheticRecordCodec(t *testing.T) {
	runID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	seq := uint64(42)

	var rec SyntheticRecord
	rec.FillDeterministicPayload(runID, seq)

	buf := make([]byte, RecordHeaderSize+FixedPayloadSize)
	n, err := rec.Encode(buf)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("encoded %d bytes, expected %d", n, len(buf))
	}

	var decoded SyntheticRecord
	if err := decoded.Decode(buf); err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	if decoded.RunID != runID {
		t.Fatalf("expected RunID %v, got %v", runID, decoded.RunID)
	}
	if decoded.SeqNum != seq {
		t.Fatalf("expected SeqNum %d, got %d", seq, decoded.SeqNum)
	}
	if decoded.ValidLen != rec.ValidLen {
		t.Fatalf("expected ValidLen %d, got %d", rec.ValidLen, decoded.ValidLen)
	}
	if !bytes.Equal(decoded.Payload[:], rec.Payload[:]) {
		t.Fatalf("payload mismatch")
	}
}

func TestSyntheticRecordErrors(t *testing.T) {
	t.Run("buffer too small to encode", func(t *testing.T) {
		var rec SyntheticRecord
		buf := make([]byte, 10)
		_, err := rec.Encode(buf)
		if !errors.Is(err, ErrBufferTooSmall) {
			t.Fatalf("expected ErrBufferTooSmall, got %v", err)
		}
	})

	t.Run("buffer too small to decode", func(t *testing.T) {
		var rec SyntheticRecord
		buf := make([]byte, 10)
		err := rec.Decode(buf)
		if !errors.Is(err, ErrTruncatedRecord) {
			t.Fatalf("expected ErrTruncatedRecord, got %v", err)
		}
	})

	t.Run("invalid magic", func(t *testing.T) {
		var rec SyntheticRecord
		buf := make([]byte, RecordHeaderSize+FixedPayloadSize)
		buf[0] = 0xFF // wrong magic
		err := rec.Decode(buf)
		if !errors.Is(err, ErrInvalidMagic) {
			t.Fatalf("expected ErrInvalidMagic, got %v", err)
		}
	})
}

func TestKeyMapping(t *testing.T) {
	for seq := uint64(0); seq < 32; seq++ {
		key := KeyForSeq(seq)
		expectedBucket := seq % NumKeyBuckets
		expected := bytes.Equal(key, []byte(string(KeyForSeq(expectedBucket))))
		if !expected {
			t.Fatalf("key mapping mismatch for seq %d", seq)
		}
	}
}

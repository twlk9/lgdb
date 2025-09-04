package keys

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// UserKey represents a user-provided key (raw bytes without sequence/kind)
type UserKey []byte

// CompareToInternal compares this user key to an internal key by extracting
// the user key portion from the internal key and doing a simple bytes comparison
func (uk UserKey) CompareToInternal(internalKey []byte) int {
	if len(internalKey) < KeyFootLen {
		return bytes.Compare([]byte(uk), internalKey) // Fallback to full comparison
	}
	// Extract user key portion (everything except the last KeyFootLen bytes)
	internalUserKey := internalKey[:len(internalKey)-KeyFootLen]
	return bytes.Compare([]byte(uk), internalUserKey)
}

// Compare compares two user keys
func (uk UserKey) Compare(other UserKey) int {
	return bytes.Compare([]byte(uk), []byte(other))
}

// String returns the string representation of the user key
func (uk UserKey) String() string {
	return string(uk)
}

var (
	// ErrCorruption is returned when data corruption is detected
	ErrCorruption = errors.New("data corruption detected")
)

// Kind represents the type of a key-value operation.
// This tells us whether a key is being set, deleted, or range-deleted.
type Kind uint8

// Range represents iteration bounds.
// Used for bounded iteration - Start is inclusive, Limit is exclusive.
type Range struct {
	Start EncodedKey // Inclusive lower bound (nil = no lower bound)
	Limit EncodedKey // Exclusive upper bound (nil = no upper bound)
}

// New range takest the user keys and turns them into internal keys
// used for searching bounds checking
func NewRange(start, limit UserKey) *Range {
	sk := NewEncodedKey(start, MaxSequenceNumber, KindSeek)
	lk := NewEncodedKey(limit, MaxSequenceNumber, KindSeek)
	r := &Range{Start: sk, Limit: lk}
	return r
}

const (
	// KindSet indicates a set operation
	KindSet Kind = 1

	// KindDelete indicates a delete operation (tombstone)
	KindDelete Kind = 2

	// KindRangeDelete indicates a range deletion operation
	KindRangeDelete Kind = 3

	// KindSeek is used for seeking to a particular sequence number
	// Following LevelDB pattern: use highest-numbered type for seeks
	KindSeek Kind = 4

	// KeyFootLen is the constant number of bits that represent a
	// key's footer. 56 bits for the seqence and the trailing bit for
	// Kind
	KeyFootLen = 8

	// MaxSequenceNumber is the maximum possible sequence number
	// Following LevelDB: (1 << 56) - 1
	MaxSequenceNumber = (uint64(1) << 56) - 1
)

// IsValidUserKey checks if a user key is valid.
// Must be non-empty and not too big (we don't want massive keys).
func IsValidUserKey(key UserKey) bool {
	return len(key) > 0 && len(key) <= 1024*1024 // Max 1MB key size
}

// IsValidValue checks if a value is valid.  Values can be empty but
// not too big (1GB limit).
func IsValidValue(value []byte) bool {
	return len(value) <= 1024*1024*1024 // Max 1GB value size
}

type EncodedKey []byte

func NewEncodedKey(key []byte, seq uint64, kind Kind) EncodedKey {
	size := len(key) + KeyFootLen
	b := make([]byte, size)
	copy(b, key)
	// pack kind into first byte of seq (makes seq 56 bits)
	p := (seq << 8) | uint64(kind)
	binary.LittleEndian.PutUint64(b[len(key):len(key)+KeyFootLen], p)
	return b
}

// NewQueryKey creates a new internal key with MaxSequenceNum and
// KindSeek set already. Convenience mostly for testing.
func NewQueryKey(userKey []byte) EncodedKey {
	return NewEncodedKey(userKey, MaxSequenceNumber, KindSeek)
}

func (ek EncodedKey) Encode(key []byte, seq uint64, kind Kind) {
	copy(ek, key)
	p := (seq << 8) | uint64(kind)
	binary.LittleEndian.PutUint64(ek[len(key):len(key)+KeyFootLen], p)
}

func (ek EncodedKey) UserKey() UserKey {
	return UserKey(ek[:len(ek)-KeyFootLen])
}

func (ek EncodedKey) Seq() uint64 {
	offset := len(ek) - KeyFootLen
	p := binary.LittleEndian.Uint64(ek[offset:])
	return p >> 8
}

func (ek EncodedKey) Kind() Kind {
	offset := len(ek) - KeyFootLen
	p := binary.LittleEndian.Uint64(ek[offset:])
	return Kind(p & 0xff)
}

func (ek EncodedKey) Compare(o EncodedKey) int {
	uk := ek.UserKey()
	ok := o.UserKey()

	ukcmp := bytes.Compare(uk, ok)
	if ukcmp != 0 {
		return ukcmp
	}

	if ek.Seq() > o.Seq() {
		return -1
	} else if ek.Seq() < o.Seq() {
		return 1
	}

	if ek.Kind() < o.Kind() {
		return -1
	} else if ek.Kind() > o.Kind() {
		return 1
	}
	return 0
}

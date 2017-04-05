package borm

import (
	"encoding/binary"
	"encoding/hex"
	"sync/atomic"
	"time"
)

var (
	// idCounter is atomically incremented when generating a new ObjectId
	// using GenerateID() function. It's used as a counter part of an id.
	idCounter uint32
)

// GenerateID returns a new unique ObjectId.
func GenerateID() string {
	return CreateID(time.Now(), atomic.AddUint32(&idCounter, 1))
}

// CreateID create a unique ObjectId.
func CreateID(t time.Time, count uint32) string {
	var b [8]byte
	// Timestamp, 4 bytes, big endian
	binary.BigEndian.PutUint32(b[:], uint32(t.UTC().Unix()))
	// idCounter, 4 bytes, big endian
	binary.BigEndian.PutUint32(b[4:], count)
	return hex.EncodeToString(b[:])
}

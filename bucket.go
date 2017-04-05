package borm

import "time"

// Bucket is the Interface to implement to skip reflect calls on all data passed into the bolthold
type Bucket struct {
	store  *Store
	Name   string
	name   []byte
	encode EncodeFunc
	decode DecodeFunc
}

// Record is a data record
type Record interface {
	Time() time.Time
}

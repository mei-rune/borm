// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm

import (
	"bytes"
	"errors"

	"github.com/boltdb/bolt"
)

// ErrNotFound is returned when no data is found for the given key
var ErrNotFound = errors.New("No data found for this key")
var ErrBucketNotFound = errors.New("No data found for this key")

// Get retrieves a value from borm and puts it into result.  Result must be a pointer
func (b *Bucket) Get(key string, result interface{}) error {
	return b.store.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		value := bkt.Get([]byte(key))
		if value == nil {
			return ErrNotFound
		}

		return b.decode(value, result)
	})
}

// GetRange retrieves a set of values from the bolt that matches the key range.
func (b *Bucket) GetRange(start, end string, cb func(it *Iterator) error) error {
	return b.store.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		var it = Iterator{
			B:        b,
			Cursor:   bkt.Cursor(),
			startKey: []byte(start),
			endKey:   []byte(end),
			isFirst:  true,
		}

		return cb(&it)
	})
}

// ForEach retrieves all values from the bolt.
func (b *Bucket) ForEach(cb func(it *Iterator) error) error {
	return b.store.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		var it = Iterator{
			B:       b,
			Cursor:  bkt.Cursor(),
			isFirst: true,
		}

		return cb(&it)
	})
}

type Iterator struct {
	B        *Bucket
	Cursor   *bolt.Cursor
	startKey []byte
	endKey   []byte
	isFirst  bool

	key   []byte
	value []byte
}

func (it *Iterator) Next() bool {
	if !it.isFirst {
		it.key, it.value = it.Cursor.Next()
	} else {
		if it.startKey != nil {
			it.key, it.value = it.Cursor.Seek(it.startKey)
		} else {
			it.key, it.value = it.Cursor.First()
		}
		it.isFirst = false
	}
	if it.key == nil {
		return false
	}
	if it.endKey == nil {
		return true
	}
	return bytes.Compare(it.key, it.endKey) <= 0
}

func (it *Iterator) Read(value interface{}) error {
	return it.B.decode(it.value, value)
}

func (it *Iterator) ReadWith(value interface{}, decoder DecodeFunc) error {
	return decoder(it.value, value)
}

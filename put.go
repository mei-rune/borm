// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm

import (
	"errors"

	"github.com/boltdb/bolt"
)

// ErrKeyExists is the error returned when data is being Inserted for a Key that already exists
var ErrKeyExists = errors.New("This Key already exists in this bolthold for this type")

// Insert inserts the passed in data into the the bolthold
// If the the key already exists in the bolthold, then an ErrKeyExists is returned
func (b *Bucket) Insert(key string, data interface{}) error {
	return b.store.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}
		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		gk := []byte(key)
		if existing := bkt.Get(gk); existing != nil {
			return ErrKeyExists
		}

		bs, err := b.encode(data)
		if err != nil {
			return err
		}

		return bkt.Put(gk, bs)
	})
}

// Update updates an existing record in the bolthold
// if the Key doesn't already exist in the store, then it fails with ErrNotFound
func (b *Bucket) Update(key string, data interface{}) error {
	return b.store.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}
		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		gk := []byte(key)
		if existing := bkt.Get(gk); existing == nil {
			return ErrNotFound
		}

		bs, err := b.encode(data)
		if err != nil {
			return err
		}

		return bkt.Put(gk, bs)
	})
}

// Upsert inserts the record into the bolthold if it doesn't exist.  If it does already exist, then it updates
// the existing record
func (b *Bucket) Upsert(key string, data interface{}) error {
	return b.store.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}
		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		bs, err := b.encode(data)
		if err != nil {
			return err
		}

		return bkt.Put([]byte(key), bs)
	})
}

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

type Updater interface {
	Insert(key string, data interface{}) error
	Update(key string, data interface{}) error
	Upsert(key string, data interface{}) error
}

type txUpdater struct {
	b   *Bucket
	tx  *bolt.Tx
	bkt *bolt.Bucket
}

// Insert inserts the passed in data into the the bolthold
// If the the key already exists in the bolthold, then an ErrKeyExists is returned
func (u *txUpdater) Insert(key string, data interface{}) error {
	gk := []byte(key)
	if existing := u.bkt.Get(gk); existing != nil {
		return ErrKeyExists
	}

	bs, err := u.b.encode(data)
	if err != nil {
		return err
	}

	return u.bkt.Put(gk, bs)
}

// Update updates an existing record in the bolthold
// if the Key doesn't already exist in the store, then it fails with ErrNotFound
func (u *txUpdater) Update(key string, data interface{}) error {
	gk := []byte(key)
	if existing := u.bkt.Get(gk); existing == nil {
		return ErrNotFound
	}

	bs, err := u.b.encode(data)
	if err != nil {
		return err
	}

	return u.bkt.Put(gk, bs)
}

// Upsert inserts the record into the bolthold if it doesn't exist.  If it does already exist, then it updates
// the existing record
func (u *txUpdater) Upsert(key string, data interface{}) error {
	bs, err := u.b.encode(data)
	if err != nil {
		return err
	}

	return u.bkt.Put([]byte(key), bs)
}

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

// Upsert inserts the record into the bolthold if it doesn't exist.  If it does already exist, then it updates
// the existing record
func (b *Bucket) Write(cb func(store Updater) error) error {
	return b.store.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}
		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		return cb(&txUpdater{
			b:   b,
			tx:  tx,
			bkt: bkt,
		})
	})
}

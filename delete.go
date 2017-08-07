// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm

import (
	"github.com/boltdb/bolt"
)

// Delete deletes a record from the bolthold, datatype just needs to be an example of the type stored so that
// the proper bucket and indexes are updated
func (b *Bucket) Delete(key string) error {
	return b.store.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}
		// delete data
		return bkt.Delete([]byte(key))
	})
}

// DeleteRange deletes all of the records that match the range
func (b *Bucket) DeleteRange(start, end string) error {
	return b.store.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		bkt := tx.Bucket(b.name)
		if bkt == nil {
			return ErrBucketNotFound
		}

		var it = &Iterator{
			B:        b,
			Cursor:   bkt.Cursor(),
			startKey: []byte(start),
			endKey:   []byte(end),
			isFirst:  true,
		}
		if start == "" {
			it.startKey = nil
		}
		if end == "" {
			it.endKey = nil
		}

		for it.Next() {
			bkt.Delete(it.Key())
		}
		return nil
	})
}

/*
// DeleteMatching deletes all of the records that match the passed in query
func (s *Store) DeleteMatching(dataType interface{}, query *Query) error {
	return s.Bolt().Update(func(tx *bolt.Tx) error {
		return s.TxDeleteMatching(tx, dataType, query)
	})
}

// TxDeleteMatching does the same as DeleteMatching, but allows you to specify your own transaction
func (s *Store) TxDeleteMatching(tx *bolt.Tx, dataType interface{}, query *Query) error {
	return deleteQuery(tx, dataType, query)
}
*/

// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm

import (
	"bytes"

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

		var (
			startKey = []byte(start)
			endKey   = []byte(end)
			cursor   = bkt.Cursor()
		)

		for k, _ := cursor.Seek(startKey); k != nil && bytes.Compare(k, endKey) <= 0; k, _ = cursor.Next() {
			bkt.Delete(k)
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

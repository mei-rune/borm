// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm

import (
	"os"

	"github.com/boltdb/bolt"
)

// Store is a bolthold wrapper around a bolt DB
type Store struct {
	db *bolt.DB
}

// Options allows you set different options from the defaults
// For example the encoding and decoding funcs which default to Gob
type Options struct {
	Encoder EncodeFunc
	Decoder DecodeFunc
}

// Open opens or creates a bolthold file.
func Open(filename string, mode os.FileMode, options *bolt.Options) (*Store, error) {
	options = fillOptions(options)
	db, err := bolt.Open(filename, mode, options)
	if err != nil {
		return nil, err
	}

	return &Store{
		db: db,
	}, nil
}

// set any unspecified options to defaults
func fillOptions(options *bolt.Options) *bolt.Options {
	if options == nil {
		options = &bolt.Options{}
	}
	return options
}

// Bolt returns the underlying Bolt DB the bolthold is based on
func (s *Store) Bolt() *bolt.DB {
	return s.db
}

// Close closes the bolt db
func (s *Store) Close() error {
	return s.db.Close()
}

// CreateBucket create a bucket
func (s *Store) GetBucket(name string, encoder EncodeFunc, decoder DecodeFunc) (*Bucket, error) {
	err := s.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		bkt := tx.Bucket([]byte(name))
		if bkt == nil {
			return ErrBucketNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if encoder == nil {
		encoder = DefaultEncode
	}
	if decoder == nil {
		decoder = DefaultDecode
	}

	return &Bucket{
		store:  s,
		Name:   name,
		name:   []byte(name),
		encode: encoder,
		decode: decoder,
	}, nil
}

// CreateBucket create a bucket
func (s *Store) CreateBucket(name string, encoder EncodeFunc, decoder DecodeFunc) (*Bucket, error) {
	err := s.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		_, err := tx.CreateBucket([]byte(name))
		return err
	})
	if err != nil {
		return nil, err
	}

	if encoder == nil {
		encoder = DefaultEncode
	}
	if decoder == nil {
		decoder = DefaultDecode
	}

	return &Bucket{
		store:  s,
		Name:   name,
		name:   []byte(name),
		encode: encoder,
		decode: decoder,
	}, nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (s *Store) CreateBucketIfNotExists(name string, encoder EncodeFunc, decoder DecodeFunc) (*Bucket, error) {
	err := s.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		_, err := tx.CreateBucketIfNotExists([]byte(name))
		return err
	})
	if err != nil {
		return nil, err
	}

	if encoder == nil {
		encoder = DefaultEncode
	}
	if decoder == nil {
		decoder = DefaultDecode
	}

	return &Bucket{
		store:  s,
		Name:   name,
		name:   []byte(name),
		encode: encoder,
		decode: decoder,
	}, nil
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
func (s *Store) DeleteBucket(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if !tx.Writable() {
			return bolt.ErrTxNotWritable
		}

		return tx.DeleteBucket([]byte(name))
	})
}

// ForEach executes a function for each bucket in the root.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller.
func (s *Store) ForEach(fn func(name string) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(key []byte, b *bolt.Bucket) error {
			return fn(string(key))
		})
	})
}

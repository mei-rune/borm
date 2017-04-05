// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

// EncodeFunc is a function for encoding a value into bytes
type EncodeFunc func(value interface{}) ([]byte, error)

// DecodeFunc is a function for decoding a value from bytes
type DecodeFunc func(data []byte, value interface{}) error

// DefaultEncode is the default encoding func for borm (Gob)
func DefaultEncode(value interface{}) ([]byte, error) {
	var buff bytes.Buffer

	en := gob.NewEncoder(&buff)

	err := en.Encode(value)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

// DefaultDecode is the default decoding func for borm (Gob)
func DefaultDecode(data []byte, value interface{}) error {
	var buff bytes.Buffer
	de := gob.NewDecoder(&buff)

	_, err := buff.Write(data)
	if err != nil {
		return err
	}

	err = de.Decode(value)
	if err != nil {
		return err
	}

	return nil
}

// JSONEncode is the default encoding func for borm (JSON)
func JSONEncode(value interface{}) ([]byte, error) {
	var buff bytes.Buffer
	err := json.NewEncoder(&buff).Encode(value)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

// JSONDecode is the default decoding func for borm (JSON)
func JSONDecode(data []byte, value interface{}) error {
	return json.Unmarshal(data, value)
}

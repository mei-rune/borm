// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm_test

import (
	"cn/com/hengwei/commons/borm"
	"io/ioutil"
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	filename := tempfile()
	store, err := borm.Open(filename, 0666, nil)
	if err != nil {
		t.Fatalf("Error opening %s: %s", filename, err)
	}

	if store == nil {
		t.Fatalf("store is null!")
	}

	defer store.Close()
	defer os.Remove(filename)
}

// copy from index.go
func indexName(typeName, indexName string) []byte {
	return []byte("_index" + ":" + typeName + ":" + indexName)
}

// utilities

// testWrap creates a temporary database for testing and closes and cleans it up when
// completed.
func testWrap(t *testing.T, tests func(store *borm.Store, t *testing.T)) {
	filename := tempfile()
	store, err := borm.Open(filename, 0666, nil)
	if err != nil {
		t.Fatalf("Error opening %s: %s", filename, err)
	}

	if store == nil {
		t.Fatalf("store is null!")
	}

	defer store.Close()
	defer os.Remove(filename)

	tests(store, t)
}

// tempfile returns a temporary file path.
func tempfile() string {
	f, err := ioutil.TempFile("", "borm-")
	if err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	if err := os.Remove(f.Name()); err != nil {
		panic(err)
	}
	return f.Name()
}

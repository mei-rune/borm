// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm_test

import (
	"cn/com/hengwei/commons/borm"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	testWrap(t, func(store *borm.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		}
		bkt, err := store.CreateBucket("bucktest", nil, nil)
		if err != nil {
			t.Fatalf("Error creating bucket for get test: %s", err)
		}

		err = bkt.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for get test: %s", err)
		}

		result := &ItemTest{}

		err = bkt.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from borm: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}
	})
}

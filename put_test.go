// Copyright 2016 Tim Shannon. All rights reserved.
// Use of this source code is governed by the MIT license
// that can be found in the LICENSE file.

package borm_test

import (
	"cn/com/hengwei/commons/borm"
	"testing"
	"time"
)

func TestInsert(t *testing.T) {
	testWrap(t, func(store *borm.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		bkt, err := store.CreateBucket("bucktest", nil, nil)
		if err != nil {
			t.Fatalf("Error creating bucket for get test: %s", err)
		}

		err = bkt.Insert(key, data)
		if err != nil {
			t.Fatalf("Error inserting data for test: %s", err)
		}

		result := &ItemTest{}

		err = bkt.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from borm: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}

		// test duplicate insert
		err = bkt.Insert(key, &ItemTest{
			Name:    "Test Name",
			Created: time.Now(),
		})

		if err != borm.ErrKeyExists {
			t.Fatalf("Insert didn't fail! Expected %s got %s", borm.ErrKeyExists, err)
		}
	})
}

func TestUpdate(t *testing.T) {
	testWrap(t, func(store *borm.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		bkt, err := store.CreateBucket("bucktest", nil, nil)
		if err != nil {
			t.Fatalf("Error creating bucket for get test: %s", err)
		}

		err = bkt.Update(key, data)
		if err != borm.ErrNotFound {
			t.Fatalf("Update without insert didn't fail! Expected %s got %s", borm.ErrNotFound, err)
		}

		err = bkt.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		result := &ItemTest{}

		err = bkt.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from borm: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		// test duplicate insert
		err = bkt.Update(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		err = bkt.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from borm: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Update didn't complete.  Expected %s, got %s", update, result)
		}
	})
}

func TestUpsert(t *testing.T) {
	testWrap(t, func(store *borm.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		bkt, err := store.CreateBucket("bucktest", nil, nil)
		if err != nil {
			t.Fatalf("Error creating bucket for get test: %s", err)
		}

		err = bkt.Upsert(key, data)
		if err != nil {
			t.Fatalf("Error upserting data: %s", err)
		}

		result := &ItemTest{}

		err = bkt.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from borm: %s", err)
		}

		if !data.equal(result) {
			t.Fatalf("Got %s wanted %s.", result, data)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		// test duplicate insert
		err = bkt.Upsert(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		err = bkt.Get(key, result)
		if err != nil {
			t.Fatalf("Error getting data from borm: %s", err)
		}

		if !result.equal(update) {
			t.Fatalf("Upsert didn't complete.  Expected %s, got %s", update, result)
		}
	})
}

/*
func TestIssue14(t *testing.T) {
	testWrap(t, func(store *borm.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		bkt, err := store.CreateBucket("bucktest", nil, nil)
		if err != nil {
			t.Fatalf("Error creating bucket for get test: %s", err)
		}

		err = bkt.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		err = bkt.Update(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		var result []ItemTest
		// try to find the record on the old index value
		err = bkt.Find(&result, borm.Where("Category").Eq("Test Category"))
		if err != nil {
			t.Fatalf("Error retrieving query result for TestIssue14: %s", err)
		}

		if len(result) != 0 {
			t.Fatalf("Old index still exists after update.  Expected %d got %d!", 0, len(result))
		}

	})
}

func TestIssue14Upsert(t *testing.T) {
	testWrap(t, func(store *borm.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}

		bkt, err := store.CreateBucket("bucktest", nil, nil)
		if err != nil {
			t.Fatalf("Error creating bucket for get test: %s", err)
		}

		err := bkt.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		update := &ItemTest{
			Name:     "Test Name Updated",
			Category: "Test Category Updated",
			Created:  time.Now(),
		}

		err = bkt.Upsert(key, update)

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		var result []ItemTest
		// try to find the record on the old index value
		err = bkt.Find(&result, borm.Where("Category").Eq("Test Category"))
		if err != nil {
			t.Fatalf("Error retrieving query result for TestIssue14: %s", err)
		}

		if len(result) != 0 {
			t.Fatalf("Old index still exists after update.  Expected %d got %d!", 0, len(result))
		}
	})
}

func TestIssue14UpdateMatching(t *testing.T) {
	testWrap(t, func(store *borm.Store, t *testing.T) {
		key := "testKey"
		data := &ItemTest{
			Name:     "Test Name",
			Category: "Test Category",
			Created:  time.Now(),
		}
		err := store.Insert(key, data)
		if err != nil {
			t.Fatalf("Error creating data for update test: %s", err)
		}

		err = store.UpdateMatching(&ItemTest{}, borm.Where("Name").Eq("Test Name"),
			func(record interface{}) error {
				update, ok := record.(*ItemTest)
				if !ok {
					return fmt.Errorf("Record isn't the correct type!  Wanted Itemtest, got %T", record)
				}

				update.Category = "Test Category Updated"

				return nil
			})

		if err != nil {
			t.Fatalf("Error updating data: %s", err)
		}

		var result []ItemTest
		// try to find the record on the old index value
		err = store.Find(&result, borm.Where("Category").Eq("Test Category"))
		if err != nil {
			t.Fatalf("Error retrieving query result for TestIssue14: %s", err)
		}

		if len(result) != 0 {
			t.Fatalf("Old index still exists after update.  Expected %d got %d!", 0, len(result))
		}

	})
}
*/

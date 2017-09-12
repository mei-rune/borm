package borm

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"time"
)

type Shard struct {
	path               string
	startTime, endTime time.Time
}

// Shards is a slice of Shards.
type Shards []*Shard

// Shards are ordered by decreasing end time.
// If two shards have the same end time, then order by decreasing start time.
// This means that the first index in the slice covers the latest time range.
func (i Shards) Len() int { return len(i) }
func (i Shards) Less(u, v int) bool {
	if i[u].endTime.After(i[v].endTime) {
		return true
	}
	return i[u].startTime.After(i[v].startTime)
}
func (i Shards) Swap(u, v int) { i[u], i[v] = i[v], i[u] }

func openShard(path string, loc *time.Location) (*Shard, error) {
	name := filepath.Base(path)
	idx := strings.IndexRune(name, '.')
	if idx >= 0 {
		name = name[:idx]
	}

	ss := strings.Split(name, "_")
	if len(ss) != 2 {
		return nil, errors.New("invalid shard name - " + name)
	}

	year, err := strconv.Atoi(ss[0])
	if err != nil {
		return nil, errors.New("invalid shard name - " + name)
	}
	yearDay, err := strconv.Atoi(ss[1])
	if err != nil {
		return nil, errors.New("invalid shard name - " + name)
	}
	start := time.Date(year, time.January, 0, 0, 0, 0, 0, loc).AddDate(0, 0, yearDay)
	return &Shard{path: path,
		startTime: start,
		endTime:   start.AddDate(0, 0, 1)}, nil
}

func ListShards(path string, loc *time.Location) (Shards, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		if !os.IsExist(err) {
			return nil, err
		}
	}
	d, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open engine: %s", err.Error())
	}

	files, err := d.Readdir(0)
	if err != nil {
		return nil, err
	}
	d.Close()

	var shards Shards
	// Open all indexes.
	for _, fi := range files {
		if fi.IsDir() ||
			strings.HasPrefix(fi.Name(), ".") ||
			strings.HasSuffix(fi.Name(), ".lock") {
			continue
		}
		shardPath := filepath.Join(path, fi.Name())
		shard, err := openShard(shardPath, loc)
		if err != nil {
			return nil, fmt.Errorf("engine failed to open at shard %s: %s", shardPath, err.Error())
		}
		log.Printf("engine opened shard at %s", shardPath)
		shards = append(shards, shard)
		sort.Sort(shards)
	}

	return shards, nil
}

func removeShardsBefore(shards Shards, t time.Time) error {
	for _, shard := range shards {
		if shard.startTime.Before(t) {
			if err := os.Remove(shard.path); err != nil {
				return err
			}
		}
	}
	return nil
}

func EnforceRetention(path string, t time.Time) error {
	shards, err := ListShards(path, t.Location())
	if err != nil {
		return err
	}
	return removeShardsBefore(shards, t)
}

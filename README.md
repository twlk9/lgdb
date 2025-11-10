# lgdb - Level Go Database

An embedded key-value store. Thread-safe, minimal dependencies (just
[compression](https://github.com/klauspost/compress) at this
point). References used include
[pebble](https://github.com/cockroachdb/pebble),
[rocksdb](https://github.com/facebook/rocksdb), and
[goleveldb](https://github.com/syndtr/goleveldb)

Started as a learning project but ended up actually being useful. Did
my best to keep things simple and understandable while maintaining
performance. Easier said than done.

Doesn't have bloom filters since my main use case is range queries on
time series data. May add them as an option at some point.

## Range Deletion

I have attempted a bunch of different methods, but was unsatisfied
with the results. Current solution is to store range deletions in a
manifest like file and read into memory on open. All iterators respect
the range deletion including compactions. To keep these range
deletions from lingering in memory for too long the db attempts to
compact them away after sweeping L0 and making sure no other levels
are over their size limit. The compaction manager will select the
oldest range deletion and perform a same level compaction by rewriting
any sstable with overlapping keys and containing a lower sequence
(without the keys covered by the range deletion of course). After
clearing all levels of possible overlap the range deletion is
removed. Still needs more testing, but initially seems to perform well
for my workload.

## Tiered Compression

More a sliding range where you can set lower levels to one compression
and higher levels to a different one. Run hot data (L0-L2) through
fast S2 compression and let the cold data (L3+) get hit with Zstd for
space efficiency. Or just pick one compression method for everything
if you prefer.

## Quick Start

```go
package main

import (
	"fmt"
	"log"

	"github.com/twlk9/lgdb"
)

func main() {
	// Open database
	opts := lgdb.DefaultOptions()
	opts.Path = "/tmp/mydb"
	db, err := lgdb.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Put a value
	err = db.Put([]byte("name"), []byte("Alice"))
	if err != nil {
		log.Fatal(err)
	}

	// Get a value
	value, err := db.Get([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("name = %s\n", value)

	// Delete a key
	err = db.Delete([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}

	// Iterate over all keys
	iter := db.NewIterator(nil)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("%s = %s\n", key, value)
	}

	// Iterate over key range
	rangeIter, err := db.Scan([]byte("key2"), []byte("key4"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer rangeIter.Close()
	for rangeIter.SeekToFirst(); rangeIter.Valid(); rangeIter.Next() {
		fmt.Printf("%s\n", rangeIter.Key())
	}

	// Iterate over a prefix
	prefixIter, err := db.ScanPrefix([]byte("user:"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer prefixIter.Close()

	for prefixIter.SeekToFirst(); prefixIter.Valid(); prefixIter.Next() {
		fmt.Printf("%s\n", prefixIter.Key())
	}
}
```

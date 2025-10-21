# lgdb - Level Go Database

An embedded key-value store. Thread-safe, minimal dependencies (just
compression at this point).

Started as a learning project but ended up actually being useful. Did
my best to keep things simple and understandable while maintaining
performance.

Doesn't have bloom filters since my main use case is range queries on
time series data. No range deletion support yet either. I tried a few
approaches but haven't landed on something I'm happy with yet. Will
keep trying.

It does have is tiered compression. Run hot data (L0-L2) through fast
S2 compression and let the cold data (L3+) get hit with Zstd for space
efficiency. Or just pick one compression method for everything if you
prefer.

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

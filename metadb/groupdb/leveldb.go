package groupdb

import (
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"local/orderbook/metadb"
	"sync"
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type levelDB struct {
	fn string      // filename for reporting
	db *leveldb.DB // levelDB instance

	quitLock sync.Mutex // Mutex protecting the quit channel access

	logger *log.Entry // Contextual logger tracking the database path
}

// newLevelDB returns a wrapped levelDB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
// The customize function allows the caller to modify the leveldb options.
func newLevelDB(file string) (*levelDB, error) {
	logger := log.WithField("dbpath", file)

	// Open the ethdb and recover any potential corruptions
	db, err := leveldb.OpenFile(file, &opt.Options{
		Filter:      filter.NewBloomFilter(10),
		WriteBuffer: 4 * 1024 * 1024,
	})
	//db, err := leveldb.OpenFile(file, options)
	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(file, nil)
	}
	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	ldb := &levelDB{
		fn:     file,
		db:     db,
		logger: logger,
	}

	return ldb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *levelDB) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	return db.db.Close()
}

// Has retrieves if a key is present in the key-value store.
func (db *levelDB) Has(k []byte) (bool, error) {
	return db.db.Has(k, nil)
}

// Get retrieves the given key if it's present in the key-value store.
func (db *levelDB) Get(k []byte) ([]byte, error) {
	dat, err := db.db.Get(k, nil)
	if err != nil {
		return nil, errors.New("not found")
	}
	return dat, nil
}

// Set inserts the given value into the key-value store.
func (db *levelDB) Set(k []byte, value []byte) error {
	return db.db.Put(k, value, nil)
}

// Delete removes the key from the key-value store.
func (db *levelDB) Delete(k []byte) error {
	return db.db.Delete(k, nil)
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *levelDB) NewBatch() metadb.Batch {
	return &batch{
		db: db.db,
		b:  new(leveldb.Batch),
	}
}

// NewBatchWithSize creates a write-only database batch with pre-allocated buffer.
func (db *levelDB) NewBatchWithSize(size int) metadb.Batch {
	return &batch{
		db: db.db,
		b:  new(leveldb.Batch),
	}
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *levelDB) NewIterator(prefix []byte, start []byte) metadb.Iterator {
	return db.db.NewIterator(bytesPrefixRange(prefix, start), nil)
}

// batch is a write-only leveldb batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	db   *leveldb.DB
	b    *leveldb.Batch
	mux  sync.Mutex
	size int
}

func (b *batch) Replay(w metadb.KeyValueWriter) error {
	return errors.New("unsupport replay")
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Set(k []byte, value []byte) error {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.b.Put(k, value)
	b.size += len(k) + len(value)
	return nil
}

// Delete inserts the a key removal into the batch for later committing.
func (b *batch) Delete(k []byte) error {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.b.Delete(k)
	b.size += len(k)
	return nil
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.b.Put(key, value)
	b.size += len(key) + len(value)
	return nil
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	b.mux.Lock()
	defer b.mux.Unlock()
	return b.db.Write(b.b, nil)
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.mux.Lock()
	defer b.mux.Unlock()
	b.b.Reset()
	b.size = 0
}

// bytesPrefixRange returns key range that satisfy
// - the given prefix, and
// - the given seek position
func bytesPrefixRange(prefix, start []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Start = append(r.Start, start...)
	return r
}

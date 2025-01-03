package groupdb

import (
	"errors"
	"sync"

	"local/orderbook/metadb"
)

var (
	_ metadb.Batch = &gBatch{}
)

type gBatch struct {
	gdb     *GroupDb
	batches []metadb.Batch
}

func (g *gBatch) instance(k []byte) metadb.Batch {
	if len(k) == 0 {
		return g.batches[0]
	}
	idx := int(k[len(k)-1]) % len(g.batches)
	return g.batches[idx]
}

func (g *gBatch) Put(key []byte, value []byte) error {
	return g.instance(key).Put(key, value)
}

func (g *gBatch) Delete(key []byte) error {
	return g.instance(key).Delete(key)
}

func (g *gBatch) ValueSize() int {
	v := 0
	for _, b := range g.batches {
		v += b.ValueSize()
	}
	return v
}

func (g *gBatch) Write() error {
	wg := sync.WaitGroup{}
	wg.Add(len(g.batches))
	var dberr error
	for _, b := range g.batches {
		go func(b metadb.Batch) {
			defer wg.Done()
			if err := b.Write(); err != nil {
				dberr = err
			}
		}(b)
	}
	wg.Wait()
	return dberr
}

func (g *gBatch) Reset() {
	for _, b := range g.batches {
		b.Reset()
	}
}

func (g *gBatch) Replay(w metadb.KeyValueWriter) error {
	return errors.New("replay not support")
}

func newGBatch(gdb *GroupDb) *gBatch {
	g := &gBatch{
		gdb:     gdb,
		batches: make([]metadb.Batch, gdb.dbcnt),
	}
	for i := 0; i < gdb.dbcnt; i++ {
		g.batches[i] = gdb.dbIns[i].NewBatch()
	}
	return g
}

func newGBatchWithSize(gdb *GroupDb, size int) *gBatch {
	g := &gBatch{
		gdb:     gdb,
		batches: make([]metadb.Batch, gdb.dbcnt),
	}
	for i := 0; i < gdb.dbcnt; i++ {
		g.batches[i] = gdb.dbIns[i].NewBatchWithSize(size)
	}
	return g
}

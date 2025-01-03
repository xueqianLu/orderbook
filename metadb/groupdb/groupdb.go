package groupdb

import (
	"fmt"
	"local/orderbook/metadb"
	"path/filepath"
)

var (
	_ metadb.Database = (*GroupDb)(nil)
)

const (
	dbGroupNum = 16
)

func (g *GroupDb) instance(k []byte) *levelDB {
	if len(k) == 0 {
		return g.dbIns[0]
	}
	idx := int(k[len(k)-1]) % g.dbcnt
	return g.dbIns[idx]
}

type GroupDb struct {
	dbcnt int
	dbIns []*levelDB // levelDB instance
}

func NewGroupDB(root string, path string) *GroupDb {
	var err error

	gdb := &GroupDb{
		dbcnt: dbGroupNum,
		dbIns: make([]*levelDB, dbGroupNum),
	}

	for i := 0; i < len(gdb.dbIns); i++ {
		file := fmt.Sprintf("%s-%d", "db", i)
		full := filepath.Join(root, path, file)
		gdb.dbIns[i], err = newLevelDB(full)
		if err != nil {
			panic(fmt.Sprintf("create groupdb failed:%s", err))
		}
	}
	return gdb
}

func (g *GroupDb) Close() error {
	for _, db := range g.dbIns {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (g *GroupDb) Delete(k []byte) error {
	return g.instance(k).Delete(k)
}

func (g *GroupDb) Has(k []byte) (bool, error) {
	return g.instance(k).Has(k)
}

func (g *GroupDb) Set(k []byte, value []byte) error {
	return g.instance(k).Set(k, value)
}

func (g *GroupDb) Put(k []byte, value []byte) error {
	return g.instance(k).Set(k, value)
}

func (g *GroupDb) Get(k []byte) ([]byte, error) {
	return g.instance(k).Get(k)
}

func (g *GroupDb) NewBatch() metadb.Batch {
	return newGBatch(g)
}
func (g *GroupDb) NewBatchWithSize(size int) metadb.Batch {
	return newGBatchWithSize(g, size)
}

func (g *GroupDb) NewIterator(prefix []byte, start []byte) metadb.Iterator {
	return newGIterator(g, prefix, start)
}

package groupdb

import "local/orderbook/metadb"

type gIterator struct {
	gdb       *GroupDb
	iterators []metadb.Iterator
	curidx    int
}

var (
	_ metadb.Iterator = (*gIterator)(nil)
)

func newGIterator(g *GroupDb, prefix []byte, start []byte) *gIterator {
	gi := &gIterator{
		gdb:       g,
		curidx:    0,
		iterators: make([]metadb.Iterator, g.dbcnt),
	}
	for i := 0; i < len(gi.iterators); i++ {
		gi.iterators[i] = g.dbIns[i].NewIterator(prefix, start)
	}
	return gi
}
func (g *gIterator) Next() bool {
	iter := g.iterators[g.curidx]
	if iter.Next() == false {
		g.curidx++
		if g.curidx >= len(g.iterators) {
			return false
		}
		return g.Next()
	} else {
		return true
	}
}

func (g *gIterator) Error() error {
	iter := g.iterators[g.curidx]
	return iter.Error()
}

func (g *gIterator) Key() []byte {
	iter := g.iterators[g.curidx]
	return iter.Key()
}

func (g *gIterator) Value() []byte {
	iter := g.iterators[g.curidx]
	return iter.Value()
}

func (g *gIterator) Release() {
	for _, iter := range g.iterators {
		iter.Release()
	}
}

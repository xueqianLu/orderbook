package orderbook

import (
	"github.com/google/uuid"
	"local/orderbook/metadb/groupdb"
	"sync/atomic"
)

type OrderStore struct {
	ch     chan []*Order // receive order to store.
	gdb    *groupdb.GroupDb
	quit   chan struct{}
	metric uint64
}

func NewStore(root string, path string) *OrderStore {
	return &OrderStore{
		ch:   make(chan []*Order, 1000000),
		quit: make(chan struct{}),
		gdb:  groupdb.NewGroupDB(root, path),
	}
}

func (os *OrderStore) work() {
	for {
		select {
		case <-os.quit:
			return
		case orders := <-os.ch:
			for _, order := range orders {
				d, _ := order.MarshalJSON()
				uid := uuid.New()
				_ = os.gdb.Set(uid[:], d)
				atomic.AddUint64(&os.metric, 1)
			}
		}
	}
}

func (os *OrderStore) Start() {
	go os.work()
}

func (os *OrderStore) Stop() {
	close(os.quit)
	os.gdb.Close()
}

func (os *OrderStore) Store(order *Order) {
	os.ch <- []*Order{order}
}

func (os *OrderStore) MStore(orders []*Order) {
	os.ch <- orders
}

func (os *OrderStore) Metric() uint64 {
	return atomic.LoadUint64(&os.metric)
}

package groupdb

import (
	"fmt"
	"testing"
)

func TestGroupDb_NewIterator(t *testing.T) {
	type testtype struct {
		key   string
		value string
	}
	gdb := NewGroupDB("groupdata")
	testdata := make([]testtype, 1000)
	for i := 0; i < len(testdata); i++ {
		testdata[i] = testtype{
			key:   fmt.Sprintf("key%d", i),
			value: fmt.Sprintf("value%d", i),
		}
	}
	for i := 0; i < len(testdata); i++ {
		err := gdb.Set([]byte(testdata[i].key), []byte(testdata[i].value))
		if err != nil {
			t.Error(err)
		}
	}
	iter := gdb.NewIterator([]byte("key1"), []byte("key11"))
	if iter == nil {
		t.Error("NewIterator returned nil")

	}
	cnt := 0
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("key:%s, value:%s\n", key, value)
		cnt += 1
	}
	if cnt != len(testdata) {
		t.Errorf("cnt:%d, len(testdata):%d", cnt, len(testdata))
	}

}

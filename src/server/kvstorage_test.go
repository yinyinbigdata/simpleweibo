package server

//  "server/kvstorage"
import (
    "testing"
)

//var c Cache

func testSetup() {
    //c = NewCache()
}

func TestCacheGet(t *testing.T) {
    c := NewCache()
    c.put("hello", "world")
    v := c.get("hello")
    if v != "world" {
        t.Error("get failed")
    }
}



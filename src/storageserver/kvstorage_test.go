package main

//  "server/kvstorage"
import (
    "testing"
    "fmt"
)

//var c Cache

func testSetup() {
    //c = NewCache()
}

func TestCacheGet(t *testing.T) {
    c := NewCache()
    c.put("hello", "world")
    v,_ := c.get("hello")
    if v != "world" {
        t.Error("get failed")
    }
}

func TestCacheSet(t *testing.T) {
    c := NewCache()
    
    c.put("1", "1")
    ok := c.put("1", "2")
    if ok != false {
        t.Error("set same key, not allowed")
    }
    
    keys := []string{"2", "3", "4"}
    values := []string{"two", "three", "four"}
    
    for i, k := range keys {
        ok := c.put(k, values[i])
        if ok != true {
            t.Error("put error")
        }
    }
}

func TestCacheGetList(t *testing.T) {
    c := NewCache()
    
    c.appendToList("hello", "world ")
    c.appendToList("hello", " and go ")
    
    vl := c.getList("hello")
    for _, ele := range vl {
        fmt.Printf(ele)
    }
}

func TestCachePendToList(t *testing.T) {
    c := NewCache()
    
    c.appendToList("count", "one")
    c.appendToList("count", "two")
    l := c.getList("count")
    if len(l) != 2 {
        t.Error("list size not right (2), list size %d", len(l))
    }
    c.appendToList("count", "three")
    l = c.getList("count")  
    if len(l) != 3 {
        t.Error("list size not right (3)")
    }
}

func TestCacheRemoveFromList(t *testing.T) {
    c := NewCache()
    
    c.appendToList("count", "one")
    c.appendToList("count", "two")
    l := c.getList("count")
    if len(l) != 2 {
        t.Error("list size not right (2)")
    }
    c.appendToList("count", "three")
    l = c.getList("count") 
    if len(l) != 3 {
        t.Error("list size not right (3)")
    }
    
    c.removeFromList("count", "two")
    l = c.getList("count")
    for _, ele := range l {
        fmt.Printf(ele)
        fmt.Printf(" ")
    }
    if len(l) !=2 {
        t.Error("list size not right, after remove")
    }
    

}



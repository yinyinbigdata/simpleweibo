// package server
package main

import (
    //"container/list"
    "sync"
    "log"
)

type GetArgs struct {
    Key string
}

type GetReply struct {
    Status int
    Value string
}

type GetListReply struct {
    Status int
    Value []string
}

type PutArgs struct {
    Key string
    Value string
}

type PutReply struct {
    Status int
}

type KVStorage interface {
    Get(args *GetArgs, reply *GetReply) error
    GetList(args *GetArgs, reply *GetListReply) error
    Put(args *PutArgs, reply *PutReply) error
    AppendToList(args *PutArgs, reply *PutReply) error
    RemoveFromList(args *PutArgs, reply *PutReply) error
}

type Cache struct {
    lock sync.RWMutex
    kv map[string]string
    kvl map[string][]string
}

func NewCache() *Cache {
    c := &Cache{
        kv : make(map[string]string),
        kvl : make(map[string][]string),
    }
    return c
}

func (c *Cache) get(key string) (string, bool) {
    c.lock.RLock()
    defer c.lock.RUnlock()
    value, ret := c.kv[key]
    log.Printf("cache: get %s %s", key, value)
    return value, ret
}

func (c *Cache) put(key string, value string) bool {
    c.lock.Lock()
    defer c.lock.Unlock()
    if _, e := c.kv[key]; e {
        return false
    } 
    c.kv[key] = value
    log.Printf("cache: put %s %s", key, value)
    return true
}

func (c *Cache) getList(key string) []string {
    c.lock.RLock()
    defer c.lock.RUnlock()
    log.Printf("cache: getList key %s", key)
    return c.kvl[key]
}

func (c *Cache) appendToList(key string, value string) bool {
    c.lock.Lock()
    defer c.lock.Unlock()
    if _, e := c.kvl[key]; e == false {
       c.kvl[key] = make([]string, 0)
    }
    c.kvl[key] = append(c.kvl[key], value)
    log.Printf("cache: appendToList %s %s", key, value)
    return true
}

func (c *Cache) removeFromList(key string, value string) bool {
    c.lock.Lock()
    defer c.lock.Unlock()
    var ret bool
    var l []string
    
    log.Printf("cache: removeFromList %s %s", key, value)
    
    if l, ret = c.kvl[key]; ret == false {
        return false
    }
    for i, e := range l {
        if e == value {
            l = append(l[:i], l[i+1:]...)
        }
    }
    c.kvl[key] = l
    return false
}
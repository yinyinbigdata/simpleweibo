package server

import (
    "container/list"
    "sync"
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
    kvl map[string]*list.List
}

func NewCache() *Cache {
    c := &Cache{
        kv : make(map[string]string),
        kvl : make(map[string]*list.List),
    }
    return c
}

func (c *Cache) get(key string) string {
    c.lock.RLock()
    defer c.lock.RLock()
    return c.kv[key]
}

func (c *Cache) put(key string, value string) bool {
    c.lock.Lock()
    defer c.lock.Unlock()
    if _, e := c.kv[key]; e {
        return e
    } 
    c.kv[key] = value
    return true
}

func (c *Cache) getList(key string) *list.List {
    c.lock.RLock()
    defer c.lock.RUnlock()
    return c.kvl[key]
}

func (c *Cache) appendToList(key string, value string) error {
    c.lock.Lock()
    defer c.lock.Unlock()
    if _, e := c.kvl[key]; e == false {
        c.kvl[key] = list.New()
    }
    c.kvl[key].PushFront(value)
    return nil
}
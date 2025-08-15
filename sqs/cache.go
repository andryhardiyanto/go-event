package sqs

import "sync"

type URLCache interface {
	Get(key string) (string, bool)
	Set(key, url string)
}

type MapURLCache struct {
	mu sync.RWMutex
	m  map[string]string
}

func NewMapURLCache() *MapURLCache {
	return &MapURLCache{m: make(map[string]string)}
}

func (c *MapURLCache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	u, ok := c.m[key]
	return u, ok
}

func (c *MapURLCache) Set(key, url string) {
	c.mu.Lock()
	c.m[key] = url
	c.mu.Unlock()
}

var (
	sharedCache URLCache = NewMapURLCache()
	sharedMu    sync.RWMutex
)

func SharedURLCache() URLCache {
	sharedMu.RLock()
	defer sharedMu.RUnlock()
	return sharedCache
}

func SetSharedURLCache(c URLCache) {
	if c == nil {
		return
	}
	sharedMu.Lock()
	sharedCache = c
	sharedMu.Unlock()
}

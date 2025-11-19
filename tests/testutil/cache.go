package testutil

import (
	"github.com/alicebob/miniredis/v2"
	"github.com/dnhan1707/trader/internal/cache"
)

type MiniCache struct {
	MR    *miniredis.Miniredis
	Cache *cache.Cache
}

func NewMiniCache(ttl int) (*MiniCache, error) {
	mr, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	c := cache.New(mr.Addr(), "", 0, ttl)
	return &MiniCache{MR: mr, Cache: c}, nil
}

func (m *MiniCache) Close() {
	_ = m.Cache.Close()
	m.MR.Close()
}

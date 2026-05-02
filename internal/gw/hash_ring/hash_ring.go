package hashring

import (
	"HaystackAtHome/internal/transport"
	"log/slog"
	"sort"
)

type HashFunction func(path string) int

type HashRing struct {
	servers  map[int]StorageServer
	keys     []int
	hashFunc HashFunction
}

func newGenericHashRing(conMap *ConnectionMap, hashFunc HashFunction) *HashRing {
	if conMap == nil || hashFunc == nil {
		return nil
	}
	serversNum := len(*conMap)
	ring := &HashRing{
		servers:  make(map[int]StorageServer, serversNum),
		keys:     make([]int, serversNum),
		hashFunc: hashFunc,
	}
	i := 0
	for k, v := range *conMap {
		h := hashFunc(k)
		ring.keys[i] = h
		ring.servers[h] = v
		i++
	}
	sort.Ints(ring.keys)
	return ring
}

func (self *HashRing) ChooseServer(path string) transport.SSClient {
	if len(self.keys) == 0 {
		return nil
	}
	h := self.hashFunc(path)
	idx := sort.SearchInts(self.keys, h)
	if idx == len(self.keys) {
		idx--
	}
	slog.Debug("ChooseServer", "chosen server", self.servers[self.keys[idx]].Endpoint)
	return self.servers[self.keys[idx]].Client
}

func (self *HashRing) GetKey(path string) int {
	return self.hashFunc(path)
}

func NewMd5Ring(conMap *ConnectionMap) *HashRing {
	return newGenericHashRing(conMap, md5Hash)
}

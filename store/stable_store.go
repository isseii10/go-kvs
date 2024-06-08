package store

import hraft "github.com/hashicorp/raft"

var prefixRedisAddr = []byte("___redisAddr")

func GetRedisAddrByNodeID(store hraft.StableStore, leaderID hraft.ServerID) (string, error) {
	v, err := store.Get(append(prefixRedisAddr, []byte(leaderID)...))
	if err != nil {
		return "", err
	}
	return string(v), err
}

func SetRedisAddrByNodeID(store hraft.StableStore, leaderID hraft.ServerID, addr string) error {
	return store.Set(append(prefixRedisAddr, []byte(leaderID)...), []byte(addr))
}

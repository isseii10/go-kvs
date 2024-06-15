package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/isseii10/go-kvs/raft"
	"github.com/isseii10/go-kvs/store"
	"github.com/tidwall/redcon"
)

type Redis struct {
	listen      net.Listener
	store       store.Store
	stableStore hraft.StableStore
	id          hraft.ServerID
	raft        *hraft.Raft
}

func NewRedis(id hraft.ServerID, raft *hraft.Raft, store store.Store, stableStore hraft.StableStore) *Redis {
	return &Redis{store: store, stableStore: stableStore, id: id, raft: raft}
}

func (r *Redis) Serve(addr string) error {
	var err error
	r.listen, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return r.handle()
}

func (r *Redis) handle() error {
	return redcon.Serve(
		r.listen,
		func(conn redcon.Conn, cmd redcon.Command) {
			err := r.validateCmd(cmd)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			r.processCmd(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			return true
		},
		func(conn redcon.Conn, err error) {
			if err != nil {
				log.Default().Println("error: ", err)
			}
		},
	)
}

var argsLen = map[string]int{
	"GET": 2, // GET key
	"SET": 3, // SET key value
	"DEL": 2, // DEL key
}

const (
	commandIdx = 0
	keyIdx     = 1
	valueIdx   = 2
)

func (r *Redis) validateCmd(cmd redcon.Command) error {
	if len(cmd.Args) == 0 {
		return fmt.Errorf("ERR wrong number of arguments for command")
	}
	cmdName := strings.ToUpper(string(cmd.Args[commandIdx]))
	if len(cmd.Args) != argsLen[cmdName] {
		return fmt.Errorf(`ERR wrong number of arguments for "%s" command`, cmdName)
	}
	return nil
}

func (r *Redis) processCmd(conn redcon.Conn, cmd redcon.Command) {
	ctx := context.Background()

	// check if the node is the leader
	if r.raft.State() != hraft.Leader {
		_, lid := r.raft.LeaderWithID()
		add, err := store.GetRedisAddrByNodeID(r.stableStore, lid)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteError("MOVED -1 " + add)
		return
	}
	cmdName := strings.ToUpper(string(cmd.Args[commandIdx]))
	switch cmdName {
	case "GET":
		val, err := r.store.Get(ctx, cmd.Args[keyIdx])
		if err != nil {
			switch {
			case errors.Is(err, store.ErrKeyNotFound):
				conn.WriteNull()
				return
			default:
				conn.WriteError(err.Error())
				return

			}
		}
		conn.WriteBulk(val)
	case "SET":
		kvCmd := &raft.KVCmd{
			Op:  raft.Put,
			Key: cmd.Args[keyIdx],
			Val: cmd.Args[valueIdx],
		}
		b, err := json.Marshal(kvCmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		f := r.raft.Apply(b, 1*time.Second)
		if f.Error() != nil {
			conn.WriteError(f.Error().Error())
			return
		}
		conn.WriteString("OK")
	case "DEL":
		kvCmd := &raft.KVCmd{
			Op:  raft.Del,
			Key: cmd.Args[keyIdx],
		}
		b, err := json.Marshal(kvCmd)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		f := r.raft.Apply(b, 1*time.Second)
		if f.Error() != nil {
			conn.WriteError(f.Error().Error())
			return
		}
		res := f.Response()
		err, ok := res.(error)
		if ok {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(1)
	default:
		conn.WriteError(fmt.Sprintf("ERR unknown command '%s'", cmdName))
	}
}

func (r *Redis) Close() error {
	return r.listen.Close()
}

func (r *Redis) Addr() net.Addr {
	return r.listen.Addr()
}

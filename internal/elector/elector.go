package elector

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

// Elector manages ZooKeeper-based leader election.
// All instances attempt to create the same ephemeral node;
// only the successful one is leader. Others watch and retry.
type Elector struct {
	conn       *zk.Conn
	leaderPath string
	instanceID string

	mu       sync.RWMutex
	isLeader bool

	onLeading   func(ctx context.Context)
	onFollowing func()
	cancel      context.CancelFunc
}

// New creates an Elector.
//   - servers: ZooKeeper addresses (e.g. ["localhost:2181"])
//   - path:    ZooKeeper node path (e.g. "/quote-ticker/leader")
//   - onLeading:  called when this instance becomes leader
//   - onFollowing: called when this instance becomes follower
func New(servers []string, path string, instanceID string,
	onLeading func(ctx context.Context), onFollowing func()) (*Elector, error) {

	conn, _, err := zk.Connect(servers, 5*time.Second)
	if err != nil {
		return nil, err
	}

	// Ensure parent path exists.
	ensurePath(conn, path)

	return &Elector{
		conn:        conn,
		leaderPath:  path + "/node",
		instanceID:  instanceID,
		onLeading:   onLeading,
		onFollowing: onFollowing,
	}, nil
}

// Run starts the election loop. Blocks until ctx is cancelled.
func (e *Elector) Run(ctx context.Context) {
	ctx, e.cancel = context.WithCancel(ctx)
	defer e.becomeFollower()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			e.tryElect(ctx)
			// Wait before retry, but also watch for node deletion.
			select {
			case <-ctx.Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
	}
}

func (e *Elector) tryElect(ctx context.Context) {
	// Try to create the ephemeral leader node.
	_, err := e.conn.Create(e.leaderPath,
		[]byte(e.instanceID),
		zk.FlagEphemeral,
		zk.WorldACL(zk.PermAll),
	)
	if err == nil {
		e.becomeLeader(ctx)
		return
	}

	// Node already exists — we are a follower.
	e.becomeFollower()

	// Watch for node deletion to retry.
	exists, _, ch, err := e.conn.ExistsW(e.leaderPath)
	if err != nil || !exists {
		// Node doesn't exist, retry immediately.
		return
	}

	select {
	case <-ctx.Done():
	case <-ch:
		// Node deleted, will retry on next loop iteration.
	}
}

func (e *Elector) becomeLeader(ctx context.Context) {
	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = true
	e.mu.Unlock()

	if !wasLeader {
		log.Printf("[elector] become leader (instance=%s)", e.instanceID)
		if e.onLeading != nil {
			go e.onLeading(ctx)
		}
	}
}

func (e *Elector) becomeFollower() {
	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = false
	e.mu.Unlock()

	if wasLeader {
		log.Printf("[elector] become follower (instance=%s)", e.instanceID)
		if e.onFollowing != nil {
			go e.onFollowing()
		}
	}
}

// IsLeader returns true if this instance is the current leader.
func (e *Elector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

// Close releases the ZooKeeper connection and removes the ephemeral node.
func (e *Elector) Close() {
	if e.cancel != nil {
		e.cancel()
	}
	e.conn.Delete(e.leaderPath, -1)
	e.conn.Close()
}

func ensurePath(conn *zk.Conn, path string) {
	parts := splitPath(path)
	cur := ""
	for _, p := range parts {
		cur += "/" + p
		exists, _, err := conn.Exists(cur)
		if err != nil || !exists {
			conn.Create(cur, nil, 0, zk.WorldACL(zk.PermAll))
		}
	}
}

func splitPath(path string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '/' && i > start {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	if start < len(path) {
		parts = append(parts, path[start:])
	}
	return parts
}

// Package elector provides etcd-based leader election.
// Uses etcd concurrency.Mutex with lease — the leader holds a lock key
// that auto-expires if the instance dies. Failover takes ~TTL seconds.
package elector

import (
	"context"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Elector manages etcd-based leader election.
// All instances attempt to acquire a distributed mutex on the same key;
// the holder is the leader. If the leader's session expires (crash/network
// partition), the lock is automatically released and a follower takes over.
type Elector struct {
	client     *clientv3.Client
	key        string
	instanceID string

	mu           sync.RWMutex
	isLeader     bool
	leaderCancel context.CancelFunc // cancels the onLeading context on step-down

	onLeading   func(ctx context.Context)
	onFollowing func()
}

// New creates an Elector backed by etcd.
//   - servers: etcd endpoints (e.g. ["localhost:2379"])
//   - key:     mutex key (e.g. "/quote-ticker/leader")
//   - onLeading:  called with a leader-scoped context when this instance becomes leader
//   - onFollowing: called when this instance becomes follower
func New(servers []string, key string, instanceID string,
	onLeading func(ctx context.Context), onFollowing func()) (*Elector, error) {

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   servers,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &Elector{
		client:      cli,
		key:         key,
		instanceID:  instanceID,
		onLeading:   onLeading,
		onFollowing: onFollowing,
	}, nil
}

// Run starts the election loop. Blocks until ctx is cancelled.
// Leadership changes are handled via callbacks.
func (e *Elector) Run(ctx context.Context) {
	log.Printf("[elector] starting: key=%s instance=%s", e.key, e.instanceID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		e.tryLead(ctx)
	}
}

// tryLead attempts to acquire leadership via an etcd mutex.
// - Creates a session with 5-second TTL (leader data auto-expires on crash)
// - Blocks on mutex.Lock until this instance becomes the leader
// - Then blocks until the session expires or ctx is cancelled
func (e *Elector) tryLead(ctx context.Context) {
	session, err := concurrency.NewSession(e.client,
		concurrency.WithTTL(5),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("[elector] session error: %v", err)
			time.Sleep(time.Second)
		}
		return
	}
	defer session.Close()

	mu := concurrency.NewMutex(session, e.key)

	// Block until we acquire the mutex (become leader).
	if err := mu.Lock(ctx); err != nil {
		return // context cancelled
	}

	e.becomeLeader(ctx)

	// Hold leadership until session dies or context is cancelled.
	// becomeFollower is called INSIDE the select so the transition
	// from leader→follower happens atomically with the session expiry
	// detection.  Without this, a FGC-paused leader could resume, run
	// more trades with isLeader still true, and cause a split-brain window.
	select {
	case <-ctx.Done():
		e.becomeFollower()
		mu.Unlock(context.Background())
	case <-session.Done():
		log.Printf("[elector] session expired — lost leadership")
		e.becomeFollower()
	}
}

func (e *Elector) becomeLeader(ctx context.Context) {
	e.mu.Lock()
	if e.isLeader {
		e.mu.Unlock()
		return
	}
	e.isLeader = true
	leaderCtx, leaderCancel := context.WithCancel(ctx)
	e.leaderCancel = leaderCancel
	e.mu.Unlock()

	log.Printf("[elector] become leader (instance=%s)", e.instanceID)
	if e.onLeading != nil {
		go e.onLeading(leaderCtx)
	}
}

func (e *Elector) becomeFollower() {
	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = false
	if e.leaderCancel != nil {
		e.leaderCancel()
		e.leaderCancel = nil
	}
	e.mu.Unlock()

	if wasLeader {
		log.Printf("[elector] become follower (instance=%s)", e.instanceID)
		if e.onFollowing != nil {
			go e.onFollowing()
		}
	}
}

// IsLeader returns true if this instance holds the leader lock.
func (e *Elector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

// Close releases the etcd connection.
func (e *Elector) Close() {
	if e.client != nil {
		e.client.Close()
	}
}

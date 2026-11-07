package defra

/*
#include <stdlib.h>
#include "defra.h"
*/
import "C"

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/immutable"
)

// DB is a started node's database: Go DefraDB's `client.TxnStore` and
// `client.P2P`, with its event bus, so Go DefraDB's HTTP handler serves it.
// It is safe for concurrent use.
type DB struct {
	store
	events *relay
	closed sync.Once
}

var (
	_ client.TxnStore = (*DB)(nil)
	_ client.Txn      = (*Txn)(nil)
)

// openDB is the database the node handle ptr names, its events relayed to a
// Go bus. self is the identity the node was started with, if any.
func openDB(ptr C.uintptr_t, self identity.Identity) (*DB, error) {
	events, err := startRelay(ptr)
	if err != nil {
		return nil, err
	}
	return &DB{store: store{ptr: ptr, self: self}, events: events}, nil
}

// Events is the database's event bus: update, merge-complete,
// topic-peer-event and se-artifact-received events.
func (d *DB) Events() event.Bus {
	return d.events.bus
}

// Close stops the event relay and closes the database. Closing again does
// nothing.
func (d *DB) Close() {
	d.closed.Do(func() {
		d.events.stop()
		_, _ = result(C.node_close(d.ptr), "close")
	})
}

// GetNodeIdentityToken is a bearer token of the node's identity for
// audience, valid for a day, as Go DefraDB makes it; nil when the node has
// no identity.
func (d *DB) GetNodeIdentityToken(ctx context.Context, audience immutable.Option[string]) ([]byte, error) {
	if d.self == nil {
		return nil, nil
	}
	full, ok := d.self.(identity.FullIdentity)
	if !ok || full.PrivateKey() == nil {
		return nil, identity.ErrPrivateKeyNotAvailable
	}
	return full.NewToken(24*time.Hour, audience, immutable.None[string]())
}

// RebuildCollectionIndexes does nothing: every write keeps the collection's
// indexes, so they never need rebuilding.
func (d *DB) RebuildCollectionIndexes(ctx context.Context, collectionName string) error {
	return nil
}

// NewTxn begins a transaction; its writes land when it commits.
func (d *DB) NewTxn(readOnly bool) (client.Txn, error) {
	readonly := C.int32_t(0)
	if readOnly {
		readonly = 1
	}
	r := C.begin_txn(d.ptr, readonly)
	if r.status != 0 {
		message := C.GoString(r.error)
		C.defra_free_string(r.error)
		return nil, ffiError("new_txn", message)
	}
	id := C.GoString(r.txn_id)
	C.defra_free_string(r.txn_id)
	return &Txn{store: store{ptr: d.ptr, txn: id, self: d.self}, started: time.Now()}, nil
}

// Txn is a transaction: Go's `client.Txn`, a store whose writes land when
// it commits.
type Txn struct {
	store
	started time.Time
}

// ID is the transaction's identifier.
func (t *Txn) ID() uint64 {
	id, _ := strconv.ParseUint(t.txn, 10, 64)
	return id
}

// StartTS is when the transaction began.
func (t *Txn) StartTS() time.Time {
	return t.started
}

// Commit commits the transaction.
func (t *Txn) Commit() error {
	var a cargs
	defer a.free()
	_, err := result(C.commit_txn(t.ptr, a.s(t.txn)), "commit")
	return err
}

// Discard discards the transaction: nothing it wrote lands.
func (t *Txn) Discard() {
	var a cargs
	defer a.free()
	_, _ = result(C.rollback_txn(t.ptr, a.s(t.txn)), "discard")
}

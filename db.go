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

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
)

// DB is a started node's database: Go DefraDB's `client.TxnStore` and
// `client.P2P`, so Go DefraDB's HTTP handler serves it. It is safe for
// concurrent use.
type DB struct {
	store
	events event.Bus
	closed sync.Once
}

var (
	_ client.TxnStore = (*DB)(nil)
	_ client.Txn      = (*Txn)(nil)
)

// openDB is the database the node handle ptr names.
func openDB(ptr C.uintptr_t) *DB {
	return &DB{store: store{ptr: ptr}, events: event.NewChannelBus(0, 100)}
}

// Events is an event bus nothing publishes to: the Rust node offers no
// event subscription Go's events can be relayed from.
func (d *DB) Events() event.Bus {
	return d.events
}

// Close closes the database. Closing again does nothing.
func (d *DB) Close() {
	d.closed.Do(func() {
		_, _ = result(C.node_close(d.ptr), "close")
		d.events.Close()
	})
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
	return &Txn{store: store{ptr: d.ptr, txn: id}, started: time.Now()}, nil
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

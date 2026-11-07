package defra

/*
#include <stdlib.h>
#include "defra.h"
*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/sourcenetwork/defradb/client"
)

// PrintDump writes every key/value pair of the store to standard output,
// one per line.
func (s *store) PrintDump(ctx context.Context) error {
	if err := s.outsideTxn("PrintDump"); err != nil {
		return err
	}
	var a cargs
	defer a.free()
	lines, err := call[[]string](C.print_dump(s.ptr, nil), "print_dump")
	if err != nil {
		return err
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(os.Stdout, line); err != nil {
			return err
		}
	}
	return nil
}

// MaxTxnRetries is the attempts a conflicting transaction gets.
func (d *DB) MaxTxnRetries() int {
	raw, err := result(C.max_txn_retries(d.ptr), "max_txn_retries")
	if err != nil {
		return 0
	}
	retries, _ := strconv.Atoi(raw)
	return retries
}

// PurgeNACState removes every node access control state, as the node's own
// identity, which owns node access control when it was enabled at start.
// Without NAC configured there is nothing to purge.
func (d *DB) PurgeNACState(ctx context.Context) error {
	if d.self == nil {
		status, err := d.GetNACStatus(ctx)
		if err != nil || status.Status == "not configured" {
			return err
		}
		return fmt.Errorf("defra: purge_nac_state: %w", client.ErrCanNotStartNACWithoutIdentity)
	}
	var a cargs
	defer a.free()
	_, err := result(C.purge_nac_state(d.ptr, a.s(d.self.DID())), "purge_nac_state")
	return err
}

// PurgeAndRestart, in development mode only, purges node access control,
// closes the node, deletes its store (the document access control state
// lives there) and starts it again.
func (n *Node) PurgeAndRestart(ctx context.Context) error {
	if !n.opts.EnableDevelopment {
		return client.NewErrOperationRequiresDeveloperMode("Purge")
	}
	if n.DB != nil {
		if err := n.DB.PurgeNACState(ctx); err != nil {
			return err
		}
	}
	if err := n.Close(ctx); err != nil {
		return err
	}
	n.DB, n.server, n.handler, n.APIURL = nil, nil, nil, ""
	if path := n.storePath(); path != "" {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return n.Start(ctx)
}

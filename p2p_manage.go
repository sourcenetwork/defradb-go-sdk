package defra

/*
#include <stdlib.h>
#include "defra.h"
*/
import "C"

import (
	"context"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
)

// Disconnect drops the connections to the peers at addresses.
func (s *store) Disconnect(ctx context.Context, addresses []string, opts ...options.Enumerable[options.DisconnectOptions]) error {
	o := newOptions(opts...)
	for _, address := range addresses {
		var a cargs
		_, err := result(C.p2p_disconnect(s.ptr, a.opt(did(o.Identity)), a.s(address)), "disconnect")
		a.free()
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteReplicator stops pushing the collections the options name (every
// collection when none) to the peer id names.
func (s *store) DeleteReplicator(ctx context.Context, id string, opts ...options.Enumerable[options.DeleteReplicatorOptions]) error {
	if err := s.outsideTxn("DeleteReplicator"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	collections, err := a.json(nonNil(o.CollectionNames))
	if err != nil {
		return err
	}
	_, err = result(C.p2p_delete_replicator(s.ptr, a.opt(did(o.Identity)), a.s(id), collections), "delete_replicator")
	return err
}

// ListReplicators is every replicator with the collections it pushes.
func (s *store) ListReplicators(ctx context.Context, opts ...options.Enumerable[options.ListReplicatorsOptions]) ([]client.Replicator, error) {
	if err := s.outsideTxn("ListReplicators"); err != nil {
		return nil, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	listed, err := call[[]struct {
		ID               string   `json:"id"`
		Address          string   `json:"address"`
		Collections      []string `json:"collections"`
		Status           uint8    `json:"status"`
		LastStatusChange string   `json:"last_status_change"`
	}](C.p2p_list_replicators(s.ptr, a.opt(did(o.Identity))), "list_replicators")
	if err != nil {
		return nil, err
	}
	replicators := make([]client.Replicator, len(listed))
	for i, r := range listed {
		replicators[i] = client.Replicator{
			ID:            r.ID,
			CollectionIDs: r.Collections,
			Status:        client.ReplicatorStatus(r.Status),
		}
		if r.Address != "" {
			replicators[i].Addresses = []string{r.Address}
		}
		if at, err := time.Parse(time.RFC3339Nano, r.LastStatusChange); err == nil {
			replicators[i].LastStatusChange = at
		}
	}
	return replicators, nil
}

// ListP2PCollections is the collections the node subscribes to.
func (s *store) ListP2PCollections(ctx context.Context, opts ...options.Enumerable[options.ListP2PCollectionsOptions]) ([]string, error) {
	if err := s.outsideTxn("ListP2PCollections"); err != nil {
		return nil, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	return call[[]string](C.p2p_list_collections(s.ptr, a.opt(did(o.Identity))), "list_p2p_collections")
}

// AddP2PDocuments subscribes the node to the documents docIDs names.
func (s *store) AddP2PDocuments(ctx context.Context, docIDs []string, opts ...options.Enumerable[options.AddP2PDocumentsOptions]) error {
	if err := s.outsideTxn("AddP2PDocuments"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	ids, err := a.json(nonNil(docIDs))
	if err != nil {
		return err
	}
	_, err = result(C.p2p_add_documents(s.ptr, a.opt(did(o.Identity)), ids), "add_p2p_documents")
	return err
}

// DeleteP2PDocuments unsubscribes the node from the documents docIDs names.
func (s *store) DeleteP2PDocuments(ctx context.Context, docIDs []string, opts ...options.Enumerable[options.DeleteP2PDocumentsOptions]) error {
	if err := s.outsideTxn("DeleteP2PDocuments"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	ids, err := a.json(nonNil(docIDs))
	if err != nil {
		return err
	}
	_, err = result(C.p2p_delete_documents(s.ptr, a.opt(did(o.Identity)), ids), "delete_p2p_documents")
	return err
}

// ListP2PDocuments is the documents the node subscribes to.
func (s *store) ListP2PDocuments(ctx context.Context, opts ...options.Enumerable[options.ListP2PDocumentsOptions]) ([]string, error) {
	if err := s.outsideTxn("ListP2PDocuments"); err != nil {
		return nil, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	return call[[]string](C.p2p_list_documents(s.ptr, a.opt(did(o.Identity))), "list_p2p_documents")
}

// SyncDocuments fetches the documents docIDs names from the node's peers.
// The library has no per-call block sync timeout.
func (s *store) SyncDocuments(
	ctx context.Context,
	collectionName string,
	docIDs []string,
	opts ...options.Enumerable[options.SyncDocumentsOptions],
) error {
	o := newOptions(opts...)
	if o.BlockSyncTimeout.HasValue() {
		return errNotSupported("SyncDocuments with a BlockSyncTimeout")
	}
	if err := s.outsideTxn("SyncDocuments"); err != nil {
		return err
	}
	var a cargs
	defer a.free()
	ids, err := a.json(nonNil(docIDs))
	if err != nil {
		return err
	}
	_, err = result(C.p2p_sync_documents(s.ptr, a.opt(did(o.Identity)), a.s(collectionName), ids), "sync_documents")
	return err
}

// SyncCollectionVersions fetches the collection versions versionIDs names
// from the node's peers.
func (s *store) SyncCollectionVersions(
	ctx context.Context,
	versionIDs []string,
	opts ...options.Enumerable[options.SyncCollectionVersionsOptions],
) error {
	if err := s.outsideTxn("SyncCollectionVersions"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	ids, err := a.json(nonNil(versionIDs))
	if err != nil {
		return err
	}
	_, err = result(C.p2p_sync_collection_versions(s.ptr, a.opt(did(o.Identity)), ids), "sync_collection_versions")
	return err
}

// SyncBranchableCollection fetches the branchable collection collectionID
// names from the node's peers.
func (s *store) SyncBranchableCollection(
	ctx context.Context,
	collectionID string,
	opts ...options.Enumerable[options.SyncBranchableCollectionOptions],
) error {
	if err := s.outsideTxn("SyncBranchableCollection"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	_, err := result(C.p2p_sync_branchable_collection(s.ptr, a.opt(did(o.Identity)), a.s(collectionID)), "sync_branchable_collection")
	return err
}

// nonNil is v, or an empty slice for nil, so it marshals as [] and not null.
func nonNil[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}

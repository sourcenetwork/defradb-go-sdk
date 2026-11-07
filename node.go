package defra

/*
#include <stdlib.h>
#include "defra.h"
extern int defraSDKReplicationFilterShim(uintptr_t context, const char *collection_id,
                                         const char *doc_id, const char *fields_json);
*/
import "C"

import (
	"context"
	"errors"
	gohttp "net/http"
	"runtime/cgo"
	"unsafe"

	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	defrahttp "github.com/sourcenetwork/defradb/http"
	"github.com/sourcenetwork/defradb/node"
)

// Node is a defradb.rs node in this process, shaped as Go DefraDB's
// `node.Node`: build it with New, set ReplicationFilter and P2PResources,
// then Start it. DB is set once it has started.
type Node struct {
	// DB is the database; nil until Start.
	DB *DB
	// APIURL is where Go DefraDB's HTTP API serves DB, once started, unless
	// the API is disabled.
	APIURL string
	// ReplicationFilter refuses replicated documents before anything of them
	// is stored. Set it between New and Start.
	ReplicationFilter ReplicationFilter
	// P2PResources bounds the libp2p host's resources. Set it between New and
	// Start.
	P2PResources P2PResources

	opts    *options.NodeOptions
	handles []cgo.Handle
	server  *defrahttp.Server
	handler *defrahttp.Handler
	apiErr  chan error
}

// P2PResources are the libp2p resource manager's budgets; zero keeps the
// default.
type P2PResources struct {
	// MemoryMiB is the memory budget in MiB.
	MemoryMiB uint64
	// FileDescriptors is the file descriptor budget.
	FileDescriptors uint64
	// MaxStreamsPerPeer is the streams one peer may hold open.
	MaxStreamsPerPeer uint64
}

// New resolves opts over Go DefraDB's defaults; nothing opens until Start.
// A P2P option the Rust node does not offer is an error naming it.
func New(ctx context.Context, opts ...options.Enumerable[options.NodeOptions]) (*Node, error) {
	resolved := node.DefaultNodeOptions()
	applyOptions(&resolved, opts...)
	if len(resolved.P2P.PrivateKey) > 0 {
		return nil, errNotSupported("P2P option PrivateKey")
	}
	return &Node{opts: &resolved, apiErr: make(chan error, 1)}, nil
}

// APIError receives at most one error: the HTTP API's server stopping for
// any reason but Close, as Go DefraDB's `node.Node.APIError` does. It is
// never closed.
func (n *Node) APIError() <-chan error {
	return n.apiErr
}

// Options are the node's resolved options.
func (n *Node) Options() *options.NodeOptions {
	return n.opts
}

// Start opens the database, its P2P system when P2P is enabled and given a
// listen address, and Go DefraDB's HTTP API over it unless the API is
// disabled.
//
// Of the options it honours the store path and in-memory store, DisableP2P,
// DisableAPI, the HTTP options, the DB identity (which signs the blocks the
// node writes when signing is on), the searchable encryption key, node
// access control, the remote document ACP (its log ID, addresses and signer), and every
// P2P option but EnableClearBackoffOnRetry and PrivateKey (New refuses a
// PrivateKey). Options the Rust node has no equivalent for are ignored.
func (n *Node) Start(ctx context.Context) error {
	o := n.opts
	if o.NodeACP.IsEnabled && !o.DB.Identity.HasValue() {
		return client.ErrCanNotStartNACWithoutIdentity
	}
	initFFI()
	var a cargs
	defer a.free()

	var c C.struct_NodeInitOptions
	if o.Store.Path != "" && !o.Store.BadgerInMemory && o.Store.Store != options.NodeMemoryStore {
		c.db_path = a.s(o.Store.Path)
	} else {
		c.in_memory = 1
	}
	c.iroh_discovery = 1
	if o.DB.Identity.HasValue() {
		full, ok := o.DB.Identity.Value().(identity.FullIdentity)
		if !ok {
			return errors.New("defra: the node identity has no private key")
		}
		key := full.PrivateKey()
		raw := key.Raw()
		c.signing_key_type = a.s(string(key.Type()))
		c.signing_private_key = (*C.uint8_t)(C.CBytes(raw))
		a.p = append(a.p, unsafe.Pointer(c.signing_private_key))
		c.signing_private_key_len = C.size_t(len(raw))
		if o.DB.EnableSigning {
			c.enable_signing = 1
		}
	}
	if o.DocumentACP.DocumentACPType == options.NodeRemoteDocumentACPType {
		c.vera_grpc_address = a.opt(o.DocumentACP.RemoteDACGRPCAddress)
		c.vera_comet_rpc_address = a.opt(o.DocumentACP.RemoteDACCometRPCAddress)
		c.vera_chain_id = a.opt(o.DocumentACP.RemoteDACLogID)
		if o.DocumentACP.Signer.HasValue() {
			raw := o.DocumentACP.Signer.Value().GetPrivateKey().Bytes()
			c.vera_signer_key = (*C.uint8_t)(C.CBytes(raw))
			a.p = append(a.p, unsafe.Pointer(c.vera_signer_key))
			c.vera_signer_key_len = C.size_t(len(raw))
		}
	}
	if n.ReplicationFilter != nil {
		h := cgo.NewHandle(n.ReplicationFilter)
		n.handles = append(n.handles, h)
		c.replication_filter = (*[0]byte)(C.defraSDKReplicationFilterShim)
		c.replication_filter_context = C.uintptr_t(h)
	}

	var r C.struct_NewNodeResult
	if !o.DisableP2P && len(o.P2P.ListenAddresses) > 0 {
		if err := p2pOptions(&c, &a, o.P2P, n.P2PResources); err != nil {
			n.freeHandles()
			return err
		}
		r = C.new_node_with_p2p(c, nil)
	} else {
		r = C.new_node(c)
	}
	if r.status != 0 {
		message := C.GoString(r.error)
		C.defra_free_string(r.error)
		n.freeHandles()
		return ffiError("start", message)
	}
	var self identity.Identity
	if o.DB.Identity.HasValue() {
		self = o.DB.Identity.Value()
	}
	db, err := openDB(r.node_ptr, self)
	if err != nil {
		_, _ = result(C.node_close(r.node_ptr), "close")
		n.freeHandles()
		return err
	}
	n.DB = db
	if err := n.configure(); err != nil {
		_ = n.Close(ctx)
		n.DB = nil
		return err
	}
	if o.DisableAPI {
		return nil
	}
	if err := n.startAPI(); err != nil {
		// A failed Start leaves nothing open.
		_ = n.Close(ctx)
		n.DB = nil
		return err
	}
	return nil
}

// configure applies the options the library sets on a running node: the
// searchable encryption key, and node access control with the node's
// identity as its owner the first time it is enabled.
func (n *Node) configure() error {
	o := n.opts
	var a cargs
	defer a.free()
	if key := o.DB.SearchableEncryptionKey; len(key) > 0 {
		ptr := (*C.uint8_t)(C.CBytes(key))
		a.p = append(a.p, unsafe.Pointer(ptr))
		if _, err := result(C.set_se_encryption_key(n.DB.ptr, ptr, C.uintptr_t(len(key))), "searchable_encryption_key"); err != nil {
			return err
		}
	}
	if !o.NodeACP.IsEnabled {
		return nil
	}
	status, err := n.DB.GetNACStatus(context.Background())
	if err != nil || status.Status != "not configured" {
		return err
	}
	_, err = result(C.enable_nac(n.DB.ptr, a.s(o.DB.Identity.Value().DID())), "enable_nac")
	return err
}

// startAPI serves Go DefraDB's HTTP handler over DB.
func (n *Node) startAPI() error {
	handler, err := defrahttp.NewHandler(n.DB, n.opts)
	if err != nil {
		return err
	}
	server, err := defrahttp.NewServer(handler, options.NodeHTTP().SetAll(n.opts.HTTP))
	if err != nil {
		handler.Close()
		return err
	}
	if err := server.SetListener(); err != nil {
		handler.Close()
		return err
	}
	go func() {
		if err := server.Serve(); err != nil && !errors.Is(err, gohttp.ErrServerClosed) {
			select {
			case n.apiErr <- err:
			default:
			}
		}
	}()
	n.handler, n.server, n.APIURL = handler, server, server.Address()
	return nil
}

// Close stops the API and closes the database.
func (n *Node) Close(ctx context.Context) error {
	var err error
	if n.server != nil {
		err = n.server.Shutdown(ctx)
	}
	if n.handler != nil {
		n.handler.Close()
	}
	if n.DB != nil {
		n.DB.Close()
	}
	n.freeHandles()
	return err
}

func (n *Node) freeHandles() {
	for _, h := range n.handles {
		h.Delete()
	}
	n.handles = nil
}

// p2pOptions puts Go DefraDB's P2P options into c: every listen address and
// bootstrap peer, GossipSub and the relay as Go switches them (off unless
// set), and the resource limits.
func p2pOptions(c *C.struct_NodeInitOptions, a *cargs, o options.NodeP2POptions, res P2PResources) error {
	listen, err := a.json(o.ListenAddresses)
	if err != nil {
		return err
	}
	c.p2p_listen_addrs_json = listen
	if len(o.BootstrapPeers) > 0 {
		if c.p2p_bootstrap_peers_json, err = a.json(o.BootstrapPeers); err != nil {
			return err
		}
	}
	if !o.EnablePubSub {
		c.p2p_disable_pubsub = 1
	}
	if !o.EnableRelay {
		c.p2p_disable_relay = 1
	}
	c.p2p_resource_memory_mib = C.uint64_t(res.MemoryMiB)
	c.p2p_resource_file_descriptors = C.uint64_t(res.FileDescriptors)
	c.p2p_max_streams_per_peer = C.size_t(res.MaxStreamsPerPeer)
	return nil
}

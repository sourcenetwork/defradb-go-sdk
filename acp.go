package defra

/*
#include <stdlib.h>
#include "defra.h"
*/
import "C"

import (
	"context"
	"fmt"
	"strings"

	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/immutable"
)

// AddDACPolicy registers policy, a document access control policy, as the
// option's identity.
func (s *store) AddDACPolicy(
	ctx context.Context,
	policy string,
	opts ...options.Enumerable[options.AddDACPolicyOptions],
) (client.AddPolicyResult, error) {
	if err := s.outsideTxn("AddDACPolicy"); err != nil {
		return client.AddPolicyResult{}, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	return call[client.AddPolicyResult](C.add_dac_policy(s.ptr, a.s(did(o.Identity)), a.s(policy)), "add_dac_policy")
}

// AddDACActorRelationship gives targetActor relation on the document.
func (s *store) AddDACActorRelationship(
	ctx context.Context,
	collectionName string,
	docID string,
	relation string,
	targetActor string,
	opts ...options.Enumerable[options.AddDACActorRelationshipOptions],
) (client.AddActorRelationshipResult, error) {
	if err := s.outsideTxn("AddDACActorRelationship"); err != nil {
		return client.AddActorRelationshipResult{}, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	added, err := call[struct{ Added bool }](C.add_dac_actor_relationship(
		s.ptr, a.s(did(o.Identity)), a.s(targetActor), a.s(collectionName), a.s(docID), a.s(relation),
	), "add_dac_actor_relationship")
	return client.AddActorRelationshipResult{ExistedAlready: !added.Added}, err
}

// DeleteDACActorRelationship takes relation on the document from targetActor.
func (s *store) DeleteDACActorRelationship(
	ctx context.Context,
	collectionName string,
	docID string,
	relation string,
	targetActor string,
	opts ...options.Enumerable[options.DeleteDACActorRelationshipOptions],
) (client.DeleteActorRelationshipResult, error) {
	if err := s.outsideTxn("DeleteDACActorRelationship"); err != nil {
		return client.DeleteActorRelationshipResult{}, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	deleted, err := call[struct{ Deleted bool }](C.delete_dac_actor_relationship(
		s.ptr, a.s(did(o.Identity)), a.s(targetActor), a.s(collectionName), a.s(docID), a.s(relation),
	), "delete_dac_actor_relationship")
	return client.DeleteActorRelationshipResult{RecordFound: deleted.Deleted}, err
}

// AddNACActorRelationship gives targetActor relation on the node.
func (s *store) AddNACActorRelationship(
	ctx context.Context,
	relation string,
	targetActor string,
	opts ...options.Enumerable[options.AddNACActorRelationshipOptions],
) (client.AddActorRelationshipResult, error) {
	if err := s.outsideTxn("AddNACActorRelationship"); err != nil {
		return client.AddActorRelationshipResult{}, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	added, err := call[struct{ Added bool }](C.add_nac_actor_relationship(
		s.ptr, a.s(did(o.Identity)), a.s(relation), a.s(targetActor),
	), "add_nac_actor_relationship")
	return client.AddActorRelationshipResult{ExistedAlready: !added.Added}, err
}

// DeleteNACActorRelationship takes relation on the node from targetActor.
func (s *store) DeleteNACActorRelationship(
	ctx context.Context,
	relation string,
	targetActor string,
	opts ...options.Enumerable[options.DeleteNACActorRelationshipOptions],
) (client.DeleteActorRelationshipResult, error) {
	if err := s.outsideTxn("DeleteNACActorRelationship"); err != nil {
		return client.DeleteActorRelationshipResult{}, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	deleted, err := call[struct{ Deleted bool }](C.delete_nac_actor_relationship(
		s.ptr, a.s(did(o.Identity)), a.s(relation), a.s(targetActor),
	), "delete_nac_actor_relationship")
	return client.DeleteActorRelationshipResult{RecordFound: deleted.Deleted}, err
}

// ReEnableNAC turns node access control back on after DisableNAC.
func (s *store) ReEnableNAC(ctx context.Context, opts ...options.Enumerable[options.ReEnableNACOptions]) error {
	if err := s.outsideTxn("ReEnableNAC"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	_, err := result(C.re_enable_nac(s.ptr, a.s(did(o.Identity))), "re_enable_nac")
	return err
}

// DisableNAC turns node access control off until ReEnableNAC.
func (s *store) DisableNAC(ctx context.Context, opts ...options.Enumerable[options.DisableNACOptions]) error {
	if err := s.outsideTxn("DisableNAC"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	_, err := result(C.disable_nac(s.ptr, a.s(did(o.Identity))), "disable_nac")
	return err
}

// GetNACStatus is node access control's status. It reads node state, so a
// transaction makes no difference to it.
func (s *store) GetNACStatus(
	ctx context.Context,
	opts ...options.Enumerable[options.GetNACStatusOptions],
) (client.NACStatusResult, error) {
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	info, err := call[struct {
		Status string `json:"status"`
	}](C.get_nac_status(s.ptr, a.opt(did(o.Identity))), "get_nac_status")
	return client.NACStatusResult{Status: info.Status}, err
}

// GetNodeIdentity is the node's identity, none when it has none. The
// library gives its DID; the public key comes from the identity the node was
// started with.
func (s *store) GetNodeIdentity(ctx context.Context) (immutable.Option[identity.PublicRawIdentity], error) {
	got, err := call[struct {
		DID string `json:"did"`
	}](C.get_node_identity(s.ptr), "get_node_identity")
	if err != nil && strings.Contains(err.Error(), "node identity not configured") {
		return immutable.None[identity.PublicRawIdentity](), nil
	}
	if err != nil || got.DID == "" {
		return immutable.None[identity.PublicRawIdentity](), err
	}
	if s.self != nil && s.self.DID() == got.DID {
		return immutable.Some(s.self.ToPublicRawIdentity()), nil
	}
	return immutable.Some(identity.PublicRawIdentity{DID: got.DID}), nil
}

// VerifySignature checks that pubKey signed the block blockCid names.
func (s *store) VerifySignature(
	ctx context.Context,
	blockCid string,
	pubKey crypto.PublicKey,
	opts ...options.Enumerable[options.VerifySignatureOptions],
) error {
	if pubKey == nil {
		return fmt.Errorf("defra: verify_signature: no public key")
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	keyType, key := a.s(string(pubKey.Type())), a.s(pubKey.String())
	var err error
	if s.txn != "" {
		_, err = result(C.block_verify_signature_in_txn(
			s.ptr, a.s(s.txn), keyType, key, a.s(blockCid), a.opt(did(o.Identity)),
		), "verify_signature")
	} else {
		_, err = result(C.block_verify_signature(s.ptr, keyType, key, a.s(blockCid), a.opt(did(o.Identity))), "verify_signature")
	}
	return err
}

package defra

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/immutable"
	lensmodel "github.com/sourcenetwork/lens/host-go/config/model"
)

func newIdentity(t *testing.T) identity.FullIdentity {
	t.Helper()
	id, err := identity.Generate(crypto.KeyTypeSecp256k1)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func addUser(t *testing.T, c client.Collection, value string) *client.Document {
	t.Helper()
	doc := docs(t, c, value)[0]
	if err := c.AddDocument(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func mustNotSupportedInTxn(t *testing.T, err error, method string) {
	t.Helper()
	if !errors.Is(err, ErrNotSupported) || !strings.Contains(err.Error(), method) {
		t.Fatalf("%s inside a transaction: %v", method, err)
	}
}

func TestIndexes(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	c := collectionOf(t, n.DB)
	desc, err := c.NewIndex(ctx, client.NewIndexRequest{
		Name:   "by_age",
		Fields: []client.IndexedFieldDescription{{Name: "age", Descending: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if desc.Name != "by_age" || len(desc.Fields) != 1 || desc.Fields[0].Name != "age" || !desc.Fields[0].Descending {
		t.Fatalf("%+v", desc)
	}
	listed, err := c.ListIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasIndex(listed, "by_age") {
		t.Fatalf("collection indexes %+v", listed)
	}
	all, err := n.DB.ListIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasIndex(all["User"], "by_age") {
		t.Fatalf("store indexes %+v", all)
	}
	if err := c.DeleteIndex(ctx, "by_age"); err != nil {
		t.Fatal(err)
	}
	if listed, _ := c.ListIndexes(ctx); hasIndex(listed, "by_age") {
		t.Fatalf("deleted index still listed: %+v", listed)
	}
	if _, err := c.NewIndex(ctx, client.NewIndexRequest{Name: "v", Vector: &client.VectorIndexDescription{}}); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("a vector index: %v", err)
	}
	txn, err := n.DB.NewTxn(false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard()
	_, err = collectionOf(t, txn).ListIndexes(ctx)
	mustNotSupportedInTxn(t, err, "ListIndexes")
	_, err = txn.ListIndexes(ctx)
	mustNotSupportedInTxn(t, err, "ListIndexes")
}

func hasIndex(results []client.ListIndexesResult, name string) bool {
	for _, r := range results {
		if r.Description.Name == name {
			return true
		}
	}
	return false
}

func TestEncryptedIndexes(t *testing.T) {
	n := start(t, func(nb *options.NodeOptionsBuilder) {
		nb.DB().SetSearchableEncryptionKey([]byte("0123456789abcdef0123456789abcdef"))
	}, nil)
	ctx := context.Background()
	c := collectionOf(t, n.DB)
	desc, err := c.NewEncryptedIndex(ctx, client.EncryptedIndexDescription{FieldName: "name", Type: client.EncryptedIndexTypeEquality})
	if err != nil {
		t.Fatal(err)
	}
	if desc.FieldName != "name" {
		t.Fatalf("%+v", desc)
	}
	listed, err := c.ListEncryptedIndexes(ctx)
	if err != nil || len(listed) != 1 || listed[0].FieldName != "name" {
		t.Fatalf("collection encrypted indexes %+v %v", listed, err)
	}
	all, err := n.DB.ListAllEncryptedIndexes(ctx)
	if err != nil || len(all["User"]) != 1 {
		t.Fatalf("store encrypted indexes %+v %v", all, err)
	}
	if err := c.DeleteEncryptedIndex(ctx, "name"); err != nil {
		t.Fatal(err)
	}
	if listed, err := c.ListEncryptedIndexes(ctx); err != nil || len(listed) != 0 {
		t.Fatalf("after delete %+v %v", listed, err)
	}
}

func TestDocumentExistsUpdateAndDelete(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	c := collectionOf(t, n.DB)
	alice := addUser(t, c, `{"name": "Alice", "age": 40}`)
	addUser(t, c, `{"name": "Bob", "age": 7}`)
	if ok, err := c.ExistsDocument(ctx, alice.ID()); err != nil || !ok {
		t.Fatalf("exists %v %v", ok, err)
	}
	updated, err := c.UpdateDocumentsWithFilter(ctx, `{"age": {"_gt": 10}}`, `{"age": 41}`)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Count != 1 || updated.DocIDs[0] != alice.ID().String() {
		t.Fatalf("%+v", updated)
	}
	if got := names(t, n.DB); got != `{"User":[{"age":41,"name":"Alice"},{"age":7,"name":"Bob"}]}` {
		t.Fatal(got)
	}
	if _, err := c.UpdateDocumentsWithFilter(ctx, `{}`, `[{"op": "replace", "path": "/age", "value": 1}]`); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("a JSON Patch updater: %v", err)
	}
	deleted, err := c.DeleteDocument(ctx, alice.ID())
	if err != nil || !deleted {
		t.Fatalf("delete %v %v", deleted, err)
	}
	if ok, err := c.ExistsDocument(ctx, alice.ID()); err != nil || ok {
		t.Fatalf("exists after delete %v %v", ok, err)
	}
	if deleted, err := c.DeleteDocument(ctx, alice.ID()); deleted || !errors.Is(err, client.ErrDocumentNotFoundOrNotAuthorized) {
		t.Fatalf("delete again %v %v", deleted, err)
	}
}

// signature is the signature type of the newest commit of docID, empty when
// unsigned.
func signature(t *testing.T, s execer, docID client.DocID) string {
	t.Helper()
	got := query(t, s, fmt.Sprintf(`query { _commits(docID: %q, depth: 1) { signature { type } } }`, docID))
	return got
}

func TestEnableSigningOverridesTheNode(t *testing.T) {
	id := newIdentity(t)
	n := start(t, func(nb *options.NodeOptionsBuilder) {
		nb.DB().SetNodeIdentity(id).SetEnableSigning(true)
	}, nil)
	ctx := context.Background()
	c := collectionOf(t, n.DB)
	signed := addUser(t, c, `{"name": "Alice", "age": 40}`)
	unsigned := docs(t, c, `{"name": "Bob", "age": 7}`)[0]
	if err := c.AddDocument(ctx, unsigned, options.AddDocument().SetEnableSigning(false)); err != nil {
		t.Fatal(err)
	}
	if got := signature(t, n.DB, signed.ID()); strings.Contains(got, `"signature":null`) {
		t.Fatalf("the node signs by default: %s", got)
	}
	if got := signature(t, n.DB, unsigned.ID()); !strings.Contains(got, `"signature":null`) {
		t.Fatalf("EnableSigning false leaves it unsigned: %s", got)
	}
}

func TestTruncateWithAFilter(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	c := collectionOf(t, n.DB)
	addUser(t, c, `{"name": "Alice", "age": 40}`)
	addUser(t, c, `{"name": "Bob", "age": 7}`)
	if err := c.Truncate(ctx, options.TruncateCollection().SetFilter(map[string]any{"age": map[string]any{"_lt": 10}})); err != nil {
		t.Fatal(err)
	}
	if got := names(t, n.DB); got != `{"User":[{"age":40,"name":"Alice"}]}` {
		t.Fatal(got)
	}
}

func TestCollectionManagement(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	before := collectionOf(t, n.DB).Version()
	patch := `[{"op": "add", "path": "/User/Fields/-", "value": {"Name": "email", "Kind": "String"}}]`
	if err := n.DB.PatchCollection(ctx, patch, immutable.None[lensmodel.Lens]()); err != nil {
		t.Fatal(err)
	}
	after := collectionOf(t, n.DB).Version()
	if after.VersionID == before.VersionID {
		t.Fatal("the patch made no new version")
	}
	if _, ok := after.GetFieldByName("email"); !ok {
		t.Fatalf("no email field in %+v", after.Fields)
	}
	if err := n.DB.PatchCollection(ctx, patch, immutable.Some(lensmodel.Lens{})); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("a patch with a migration: %v", err)
	}
	if _, err := n.DB.SetMigration(ctx, client.LensConfig{
		SourceCollectionVersionID:      before.VersionID,
		DestinationCollectionVersionID: after.VersionID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.DB.ListLenses(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.DB.SetActiveCollectionVersion(ctx, before.VersionID); err != nil {
		t.Fatal(err)
	}
	if got := collectionOf(t, n.DB).VersionID(); got != before.VersionID {
		t.Fatalf("active version %s, want %s", got, before.VersionID)
	}
	if err := n.DB.DeleteCollection(ctx, []string{"User"}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.DB.GetCollectionByName(ctx, "User"); !errors.Is(err, client.ErrCollectionNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestRefreshViews(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	if _, err := n.DB.AddView(ctx, `User { name }`, `type UserView @materialized(if: true) { name: String }`); err != nil {
		t.Fatal(err)
	}
	addUser(t, collectionOf(t, n.DB), `{"name": "Alice", "age": 40}`)
	if err := n.DB.RefreshViews(ctx, options.RefreshViews().SetCollectionName("UserView")); err != nil {
		t.Fatal(err)
	}
	if got := query(t, n.DB, `query { UserView { name } }`); got != `{"UserView":[{"name":"Alice"}]}` {
		t.Fatal(got)
	}
	if err := n.DB.RefreshViews(ctx, options.RefreshViews().SetVersionID("x")); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("by version ID: %v", err)
	}
}

const usersPolicy = `
description: a test policy
name: test
resources:
- name: users
  permissions:
  - name: delete
  - name: read
    expr: reader
  - name: update
  relations:
  - manages:
    - reader
    name: admin
    types:
    - actor
  - name: reader
    types:
    - actor
`

func TestDocumentAccessControl(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	owner, reader := newIdentity(t), newIdentity(t)
	policy, err := n.DB.AddDACPolicy(ctx, usersPolicy, options.AddDACPolicy().SetIdentity(owner))
	if err != nil || policy.PolicyID == "" {
		t.Fatalf("policy %+v %v", policy, err)
	}
	sdl := fmt.Sprintf(`type Secret @policy(id: %q, resource: "users") { name: String }`, policy.PolicyID)
	if _, err := n.DB.AddCollection(ctx, sdl); err != nil {
		t.Fatal(err)
	}
	c, err := n.DB.GetCollectionByName(ctx, "Secret")
	if err != nil {
		t.Fatal(err)
	}
	doc := docs(t, c, `{"name": "Alice"}`)[0]
	if err := c.AddDocument(ctx, doc, options.AddDocument().SetIdentity(owner)); err != nil {
		t.Fatal(err)
	}
	added, err := n.DB.AddDACActorRelationship(ctx, "Secret", doc.ID().String(), "reader", reader.DID(),
		options.AddDACActorRelationship().SetIdentity(owner))
	if err != nil || added.ExistedAlready {
		t.Fatalf("add %+v %v", added, err)
	}
	if ok, err := c.ExistsDocument(ctx, doc.ID(), options.ExistsDocument().SetIdentity(reader)); err != nil || !ok {
		t.Fatalf("the reader sees it: %v %v", ok, err)
	}
	removed, err := n.DB.DeleteDACActorRelationship(ctx, "Secret", doc.ID().String(), "reader", reader.DID(),
		options.DeleteDACActorRelationship().SetIdentity(owner))
	if err != nil || !removed.RecordFound {
		t.Fatalf("delete %+v %v", removed, err)
	}
	if ok, _ := c.ExistsDocument(ctx, doc.ID(), options.ExistsDocument().SetIdentity(reader)); ok {
		t.Fatal("the reader still sees it")
	}
}

func TestNodeAccessControl(t *testing.T) {
	owner, admin := newIdentity(t), newIdentity(t)
	ctx := context.Background()
	nb := options.Node()
	nb.SetDisableAPI(true)
	nb.DB().SetNodeIdentity(owner)
	nb.NodeACP().SetEnabled(true)
	n, err := New(ctx, nb)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close(ctx) })
	status, err := n.DB.GetNACStatus(ctx, options.GetNACStatus().SetIdentity(owner))
	if err != nil || status.Status != "enabled" {
		t.Fatalf("status %+v %v", status, err)
	}
	added, err := n.DB.AddNACActorRelationship(ctx, "admin", admin.DID(), options.AddNACActorRelationship().SetIdentity(owner))
	if err != nil || added.ExistedAlready {
		t.Fatalf("add %+v %v", added, err)
	}
	if err := n.DB.DisableNAC(ctx, options.DisableNAC().SetIdentity(admin)); err != nil {
		t.Fatal(err)
	}
	if status, _ := n.DB.GetNACStatus(ctx); status.Status == "enabled" {
		t.Fatal("still enabled after DisableNAC")
	}
	if err := n.DB.ReEnableNAC(ctx, options.ReEnableNAC().SetIdentity(owner)); err != nil {
		t.Fatal(err)
	}
	removed, err := n.DB.DeleteNACActorRelationship(ctx, "admin", admin.DID(), options.DeleteNACActorRelationship().SetIdentity(owner))
	if err != nil || !removed.RecordFound {
		t.Fatalf("delete %+v %v", removed, err)
	}

	nb = options.Node()
	nb.SetDisableAPI(true)
	nb.NodeACP().SetEnabled(true)
	without, err := New(ctx, nb)
	if err != nil {
		t.Fatal(err)
	}
	if err := without.Start(ctx); !errors.Is(err, client.ErrCanNotStartNACWithoutIdentity) {
		_ = without.Close(ctx)
		t.Fatalf("NAC without an identity: %v", err)
	}
}

func TestNodeIdentityTokenAndSignatures(t *testing.T) {
	id := newIdentity(t)
	n := start(t, func(nb *options.NodeOptionsBuilder) {
		nb.DB().SetNodeIdentity(id).SetEnableSigning(true)
	}, nil)
	ctx := context.Background()
	got, err := n.DB.GetNodeIdentity(ctx)
	if err != nil || !got.HasValue() || got.Value() != id.ToPublicRawIdentity() {
		t.Fatalf("identity %+v %v", got, err)
	}
	token, err := n.DB.GetNodeIdentityToken(ctx, immutable.None[string]())
	if err != nil || len(token) == 0 {
		t.Fatalf("token %q %v", token, err)
	}
	doc := addUser(t, collectionOf(t, n.DB), `{"name": "Alice", "age": 40}`)
	commits := query(t, n.DB, fmt.Sprintf(`query { _commits(docID: %q, depth: 1) { cid } }`, doc.ID()))
	cid := strings.Split(strings.Split(commits, `"cid":"`)[1], `"`)[0]
	if err := n.DB.VerifySignature(ctx, cid, id.PublicKey()); err != nil {
		t.Fatal(err)
	}
	if err := n.DB.VerifySignature(ctx, cid, newIdentity(t).PublicKey()); err == nil {
		t.Fatal("another key verified the signature")
	}

	plain := start(t, nil, nil)
	if got, err := plain.DB.GetNodeIdentity(ctx); err != nil || got.HasValue() {
		t.Fatalf("a node without an identity: %+v %v", got, err)
	}
}

func TestBackupAndActions(t *testing.T) {
	source, target := start(t, nil, nil), start(t, nil, nil)
	ctx := context.Background()
	addUser(t, collectionOf(t, source.DB), `{"name": "Alice", "age": 40}`)
	path := filepath.Join(t.TempDir(), "backup.json")
	if err := source.DB.BasicExport(ctx, path); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatalf("backup file %v %v", info, err)
	}
	if err := target.DB.BasicImport(ctx, path); err != nil {
		t.Fatal(err)
	}
	if got := names(t, target.DB); got != `{"User":[{"age":40,"name":"Alice"}]}` {
		t.Fatal(got)
	}
	actions, err := source.DB.ListActions(ctx)
	if err != nil || actions == nil {
		t.Fatalf("actions %v %v", actions, err)
	}
}

func TestEventsCarryUpdates(t *testing.T) {
	n := start(t, nil, nil)
	sub, err := n.DB.Events().Subscribe(event.UpdateName)
	if err != nil {
		t.Fatal(err)
	}
	c := collectionOf(t, n.DB)
	alice := addUser(t, c, `{"name": "Alice", "age": 40}`)
	select {
	case msg := <-sub.Message():
		update, ok := msg.Data.(event.Update)
		if !ok || update.DocID != alice.ID().String() || update.CollectionID != c.CollectionID() || !update.Cid.Defined() {
			t.Fatalf("%+v", msg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no update event")
	}
}

func TestExecRequestSubscriptions(t *testing.T) {
	n := start(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := n.DB.ExecRequest(ctx, `subscription { User { name } }`)
	if len(res.GQL.Errors) > 0 || res.Subscription == nil {
		t.Fatalf("%+v", res.GQL)
	}
	addUser(t, collectionOf(t, n.DB), `{"name": "Alice", "age": 40}`)
	select {
	case got := <-res.Subscription:
		if len(got.Errors) > 0 || !strings.Contains(fmt.Sprint(got.Data), "Alice") {
			t.Fatalf("%+v", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no subscription result")
	}
	cancel()
	for range res.Subscription {
	}
}

func TestStoreMethodsInATransaction(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	txn, err := n.DB.NewTxn(false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard()
	if _, err := txn.AddCollection(ctx, `type Later { name: String }`); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.GetCollectionByName(ctx, "Later"); err != nil {
		t.Fatalf("the transaction sees its own collection: %v", err)
	}
	if _, err := n.DB.GetCollectionByName(ctx, "Later"); err == nil {
		t.Fatal("the collection is visible before commit")
	}
	_, err = txn.AddDACPolicy(ctx, usersPolicy)
	mustNotSupportedInTxn(t, err, "AddDACPolicy")
	mustNotSupportedInTxn(t, txn.SetActiveCollectionVersion(ctx, "x"), "SetActiveCollectionVersion")
	mustNotSupportedInTxn(t, txn.AddP2PDocuments(ctx, nil), "AddP2PDocuments")
	_, err = txn.ListActions(ctx)
	mustNotSupportedInTxn(t, err, "ListActions")
	if _, err := txn.ListLenses(ctx); err != nil {
		t.Fatalf("ListLenses runs in the transaction: %v", err)
	}
}

func TestRemoteDocumentACPOptionsReachTheLibrary(t *testing.T) {
	ctx := context.Background()
	nb := options.Node()
	nb.SetDisableAPI(true)
	nb.DocumentACP().
		SetType(options.NodeRemoteDocumentACPType).
		SetLogID("vera-test").
		SetGRPCAddress("127.0.0.1:1").
		SetCometRPCAddress("127.0.0.1:2")
	n, err := New(ctx, nb)
	if err != nil {
		t.Fatal(err)
	}
	err = n.Start(ctx)
	if err == nil {
		_ = n.Close(ctx)
		t.Fatal("a remote document ACP without a signer starts")
	}
	if !strings.Contains(err.Error(), "vera_signer_key") {
		t.Fatalf("the library did not get the remote ACP options: %v", err)
	}
}

func TestP2PManagement(t *testing.T) {
	ctx := context.Background()
	listen := func(nb *options.NodeOptionsBuilder) {
		nb.P2P().SetListenAddresses("/ip4/127.0.0.1/tcp/0").SetEnablePubSub(true)
	}
	source, target := start(t, listen, nil), start(t, listen, nil)
	c := collectionOf(t, source.DB)
	alice := addUser(t, c, `{"name": "Alice", "age": 40}`)

	if err := source.DB.AddP2PDocuments(ctx, []string{alice.ID().String()}); err != nil {
		t.Fatal(err)
	}
	if ids, err := source.DB.ListP2PDocuments(ctx); err != nil || len(ids) != 1 || ids[0] != alice.ID().String() {
		t.Fatalf("p2p documents %v %v", ids, err)
	}
	if err := source.DB.DeleteP2PDocuments(ctx, []string{alice.ID().String()}); err != nil {
		t.Fatal(err)
	}
	if ids, err := source.DB.ListP2PDocuments(ctx); err != nil || len(ids) != 0 {
		t.Fatalf("p2p documents after delete %v %v", ids, err)
	}
	if err := source.DB.AddP2PCollections(ctx, []string{"User"}); err != nil {
		t.Fatal(err)
	}
	if listed, err := source.DB.ListP2PCollections(ctx); err != nil || len(listed) != 1 {
		t.Fatalf("p2p collections %v %v", listed, err)
	}

	addresses, err := target.DB.PeerInfo(ctx)
	if err != nil || len(addresses) == 0 {
		t.Fatalf("peer info %v %v", addresses, err)
	}
	if err := source.DB.AddReplicator(ctx, addresses[:1], options.AddReplicator().SetCollectionNames([]string{"User"})); err != nil {
		t.Fatal(err)
	}
	replicators, err := source.DB.ListReplicators(ctx)
	if err != nil || len(replicators) != 1 || replicators[0].ID == "" || len(replicators[0].CollectionIDs) != 1 {
		t.Fatalf("replicators %+v %v", replicators, err)
	}
	if err := source.DB.DeleteReplicator(ctx, replicators[0].ID); err != nil {
		t.Fatal(err)
	}
	if replicators, err := source.DB.ListReplicators(ctx); err != nil || len(replicators) != 0 {
		t.Fatalf("replicators after delete %+v %v", replicators, err)
	}

	if err := target.DB.Connect(ctx, mustPeerInfo(t, source.DB)[:1]); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error { return target.DB.SyncDocuments(ctx, "User", []string{alice.ID().String()}) })
	if got := names(t, target.DB); got != `{"User":[{"age":40,"name":"Alice"}]}` {
		t.Fatalf("after sync %s", got)
	}
	eventually(t, func() error { return target.DB.SyncCollectionVersions(ctx, []string{c.VersionID()}) })
	if err := target.DB.SyncDocuments(ctx, "User", []string{alice.ID().String()},
		options.SyncDocuments().SetBlockSyncTimeout(time.Second)); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("a block sync timeout: %v", err)
	}
	if err := target.DB.Disconnect(ctx, mustPeerInfo(t, source.DB)[:1]); err != nil {
		t.Fatal(err)
	}
}

func TestSyncBranchableCollection(t *testing.T) {
	ctx := context.Background()
	listen := func(nb *options.NodeOptionsBuilder) {
		nb.P2P().SetListenAddresses("/ip4/127.0.0.1/tcp/0").SetEnablePubSub(true)
	}
	source, target := start(t, listen, nil), start(t, listen, nil)
	sdl := `type Log @branchable { line: String }`
	for _, n := range []*Node{source, target} {
		if _, err := n.DB.AddCollection(ctx, sdl); err != nil {
			t.Fatal(err)
		}
	}
	logs, err := source.DB.GetCollectionByName(ctx, "Log")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := client.NewDocFromJSON(ctx, []byte(`{"line": "one"}`), logs.Version())
	if err != nil {
		t.Fatal(err)
	}
	if err := logs.AddDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	if err := target.DB.Connect(ctx, mustPeerInfo(t, source.DB)[:1]); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() error { return target.DB.SyncBranchableCollection(ctx, logs.CollectionID()) })
	if got := query(t, target.DB, `query { Log { line } }`); got != `{"Log":[{"line":"one"}]}` {
		t.Fatal(got)
	}
}

func mustPeerInfo(t *testing.T, db *DB) []string {
	t.Helper()
	addresses, err := db.PeerInfo(context.Background())
	if err != nil || len(addresses) == 0 {
		t.Fatalf("peer info %v %v", addresses, err)
	}
	return addresses
}

// eventually retries op until it succeeds or 15 seconds pass: a sync needs
// the peers' GossipSub mesh, which forms a heartbeat after they connect.
func eventually(t *testing.T, op func() error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := op()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

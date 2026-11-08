package defra

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
)

const users = "type User { name: String @index age: Int }"

// start starts a node built as Shinzo builds one, with set applied to its
// options and before to the node ahead of Start, and adds the User
// collection.
func start(t *testing.T, set func(*options.NodeOptionsBuilder), before func(*Node)) *Node {
	t.Helper()
	ctx := context.Background()
	nb := options.Node()
	nb.SetDisableAPI(true)
	if set != nil {
		set(nb)
	}
	n, err := New(ctx, nb)
	if err != nil {
		t.Fatal(err)
	}
	if before != nil {
		before(n)
	}
	if err := n.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close(context.Background()) })
	if _, err := n.DB.AddCollection(ctx, users); err != nil {
		t.Fatal(err)
	}
	return n
}

type execer interface {
	ExecRequest(context.Context, string, ...options.Enumerable[options.ExecRequestOptions]) *client.RequestResult
}

// query runs request and returns its data as JSON.
func query(t *testing.T, s execer, request string) string {
	t.Helper()
	result := s.ExecRequest(context.Background(), request)
	if len(result.GQL.Errors) > 0 {
		t.Fatal(result.GQL.Errors)
	}
	raw, err := json.Marshal(result.GQL.Data)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func names(t *testing.T, s execer) string {
	return query(t, s, `query { User(order: {name: ASC}) { name age } }`)
}

func docs(t *testing.T, collection client.Collection, values ...string) []*client.Document {
	t.Helper()
	var out []*client.Document
	for _, value := range values {
		doc, err := client.NewDocFromJSON(context.Background(), []byte(value), collection.Version())
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, doc)
	}
	return out
}

func collectionOf(t *testing.T, s client.Store) client.Collection {
	t.Helper()
	collection, err := s.GetCollectionByName(context.Background(), "User")
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func TestExecRequestAndSaveManyDocuments(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	collection := collectionOf(t, n.DB)
	if collection.Name() != "User" {
		t.Fatalf("name %q", collection.Name())
	}
	saved := docs(t, collection, `{"name": "Alice", "age": 40}`, `{"name": "Bob", "age": 7}`)
	if err := collection.(*Collection).SaveManyDocuments(ctx, saved); err != nil {
		t.Fatal(err)
	}
	if got := names(t, n.DB); got != `{"User":[{"age":40,"name":"Alice"},{"age":7,"name":"Bob"}]}` {
		t.Fatal(got)
	}
	if !saved[0].ID().IsValid() {
		t.Fatal("the save gives a new document its DocID")
	}
	// Saving a document whose DocID is stored updates it.
	if err := saved[0].Set(ctx, "age", 41); err != nil {
		t.Fatal(err)
	}
	if err := collection.SaveDocument(ctx, saved[0]); err != nil {
		t.Fatal(err)
	}
	if got := names(t, n.DB); got != `{"User":[{"age":41,"name":"Alice"},{"age":7,"name":"Bob"}]}` {
		t.Fatal(got)
	}
	result := n.DB.ExecRequest(ctx, `query { Nope { name } }`)
	if len(result.GQL.Errors) == 0 {
		t.Fatal("an unknown collection is an error")
	}
}

func TestDocumentsThroughTheCollection(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	collection := collectionOf(t, n.DB)
	alice := docs(t, collection, `{"name": "Alice", "age": 40}`)[0]
	if err := collection.AddDocument(ctx, alice); err != nil {
		t.Fatal(err)
	}
	if err := collection.AddDocument(ctx, docs(t, collection, `{"name": "Alice", "age": 40}`)[0]); err == nil {
		t.Fatal("adding a document that exists is an error")
	}
	if err := collection.AddManyDocuments(ctx, docs(t, collection, `{"name": "Bob", "age": 7}`, `{"name": "Carol", "age": 3}`)); err != nil {
		t.Fatal(err)
	}

	got, err := collection.GetDocument(ctx, alice.ID())
	if err != nil {
		t.Fatal(err)
	}
	if name, _ := got.Get("name"); name != "Alice" || got.ID() != alice.ID() {
		t.Fatalf("got %v %v", name, got.ID())
	}
	if err := got.Set(ctx, "age", 41); err != nil {
		t.Fatal(err)
	}
	if err := collection.UpdateDocument(ctx, got); err != nil {
		t.Fatal(err)
	}
	absent := docs(t, collection, `{"name": "Dave", "age": 9}`)[0]
	if err := collection.UpdateDocument(ctx, absent); !errors.Is(err, client.ErrDocumentNotFoundOrNotAuthorized) {
		t.Fatalf("updating an absent document is not found: %v", err)
	}

	deleted, err := collection.DeleteDocumentsWithFilter(ctx, map[string]any{"age": map[string]any{"_lt": 5}})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Count != 1 {
		t.Fatalf("deleted %+v", deleted)
	}
	if got := names(t, n.DB); got != `{"User":[{"age":41,"name":"Alice"},{"age":7,"name":"Bob"}]}` {
		t.Fatal(got)
	}

	all, err := n.DB.GetCollections(ctx, options.GetCollections().SetCollectionID(collection.CollectionID()))
	if err != nil || len(all) != 1 || all[0].Name() != "User" {
		t.Fatalf("collections %v %v", all, err)
	}
	if err := collection.Truncate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := names(t, n.DB); got != `{"User":[]}` {
		t.Fatal(got)
	}
	if _, err := n.DB.ExportFieldMapping(ctx, "User"); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("an unoffered method says so: %v", err)
	}
}

func TestTransactionsAndPurgeByDocIDs(t *testing.T) {
	n := start(t, nil, nil)
	ctx := context.Background()
	saved := docs(t, collectionOf(t, n.DB), `{"name": "Alice", "age": 40}`, `{"name": "Bob", "age": 7}`)
	if err := collectionOf(t, n.DB).(*Collection).SaveManyDocuments(ctx, saved); err != nil {
		t.Fatal(err)
	}

	txn, err := n.DB.NewTxn(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := collectionOf(t, txn).(*Collection).PurgeByDocIDs(ctx, []client.DocID{saved[0].ID()}, true); err != nil {
		t.Fatal(err)
	}
	if got := names(t, n.DB); !strings.Contains(got, "Alice") {
		t.Fatalf("the purge lands only on commit: %s", got)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := names(t, n.DB); got != `{"User":[{"age":7,"name":"Bob"}]}` {
		t.Fatal(got)
	}

	discarded, err := n.DB.NewTxn(false)
	if err != nil {
		t.Fatal(err)
	}
	query(t, discarded, `mutation { add_User(input: {name: "Carol", age: 1}) { _docID } }`)
	discarded.Discard()
	if got := names(t, n.DB); strings.Contains(got, "Carol") {
		t.Fatalf("a discarded transaction stores nothing: %s", got)
	}
}

func TestStartServesGoDefraDBsHTTPAPI(t *testing.T) {
	n := start(t, func(nb *options.NodeOptionsBuilder) {
		nb.SetDisableAPI(false)
		nb.HTTP().SetAddress("127.0.0.1:0")
	}, nil)
	if n.APIURL == "" {
		t.Fatal("no API URL")
	}
	query(t, n.DB, `mutation { add_User(input: {name: "Alice", age: 40}) { _docID } }`)
	body := strings.NewReader(`{"query": "query { User { name } }"}`)
	resp, err := http.Post(n.APIURL+"/api/v0/graphql", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(raw), "Alice") {
		t.Fatalf("%d %s", resp.StatusCode, raw)
	}
}

// refuseName refuses a document whose name field is the one it names, and
// counts calls that name a collection other than User.
type refuseName struct {
	name       string
	calls      atomic.Int64
	notUserCol atomic.Int64
}

func (f *refuseName) AllowReplication(ctx context.Context, collectionName, docID string, fields map[string]any) bool {
	f.calls.Add(1)
	if collectionName != "User" {
		f.notUserCol.Add(1)
	}
	return fields["name"] != f.name
}

func TestReplicationThroughTheFilter(t *testing.T) {
	ctx := context.Background()
	filter := &refuseName{name: "Mallory"}
	listen := func(nb *options.NodeOptionsBuilder) { nb.P2P().SetListenAddresses("/ip4/127.0.0.1/tcp/0") }
	source := start(t, listen, nil)
	target := start(t, listen, func(n *Node) { n.ReplicationFilter = filter })
	addresses, err := target.DB.PeerInfo(ctx)
	if err != nil || len(addresses) == 0 {
		t.Fatalf("peer info %v %v", addresses, err)
	}
	if err := source.DB.AddReplicator(ctx, addresses[:1], options.AddReplicator().SetCollectionNames([]string{"User"})); err != nil {
		t.Fatal(err)
	}
	saved := docs(t, collectionOf(t, source.DB),
		`{"name": "Alice", "age": 40}`, `{"name": "Mallory", "age": 9}`, `{"name": "Bob", "age": 7}`)
	if err := collectionOf(t, source.DB).(*Collection).SaveManyDocuments(ctx, saved); err != nil {
		t.Fatal(err)
	}
	want := `{"User":[{"age":40,"name":"Alice"},{"age":7,"name":"Bob"}]}`
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := names(t, target.DB)
		if got == want && filter.calls.Load() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the target holds %s after %d filter calls; want %s", got, filter.calls.Load(), want)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Mallory was refused, not just late.
	time.Sleep(time.Second)
	if got := names(t, target.DB); got != want {
		t.Fatal(got)
	}
	if n := filter.notUserCol.Load(); n != 0 {
		t.Fatalf("%d filter calls named a collection other than User", n)
	}
}

func TestDecodeFieldsAsGoDecodesCBOR(t *testing.T) {
	fields, err := decodeFields(`{"n": 5, "m": -5, "f": 1.5, "w": 3.0, "big": 18446744073709551615, "s": "x"}`)
	if err != nil {
		t.Fatal(err)
	}
	if fields["n"] != uint64(5) || fields["m"] != int64(-5) || fields["f"] != 1.5 || fields["s"] != "x" {
		t.Fatalf("%#v", fields)
	}
	if fields["w"] != float64(3) || fields["big"] != uint64(18446744073709551615) {
		t.Fatalf("a whole-number float stays a float and a large uint64 stays exact: %#v", fields)
	}
}

func TestUnsupportedNodeOptionsAreErrors(t *testing.T) {
	ctx := context.Background()
	nb := options.Node()
	nb.P2P().SetPrivateKey([]byte{1})
	if _, err := New(ctx, nb); !errors.Is(err, ErrNotSupported) || !strings.Contains(err.Error(), "PrivateKey") {
		t.Fatalf("a peer key: %v", err)
	}
}

func TestEveryListenAddressAndTheResourceLimits(t *testing.T) {
	n := start(t, func(nb *options.NodeOptionsBuilder) {
		nb.P2P().
			SetListenAddresses("/ip4/127.0.0.1/tcp/0", "/ip4/127.0.0.2/tcp/0").
			SetEnablePubSub(true)
	}, func(n *Node) {
		n.P2PResources = P2PResources{MemoryMiB: 256, FileDescriptors: 512, MaxStreamsPerPeer: 64}
	})
	addresses, err := n.DB.PeerInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(addresses, " ")
	if !strings.Contains(joined, "/ip4/127.0.0.1/") || !strings.Contains(joined, "/ip4/127.0.0.2/") {
		t.Fatalf("listening on %v", addresses)
	}
}

func TestCloseTwice(t *testing.T) {
	n := start(t, nil, nil)
	if err := n.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type panicFilter struct{ calls atomic.Int64 }

func (f *panicFilter) AllowReplication(context.Context, string, string, map[string]any) bool {
	f.calls.Add(1)
	panic("filter")
}

func TestAPanickingFilterKeepsTheDocument(t *testing.T) {
	ctx := context.Background()
	filter := &panicFilter{}
	listen := func(nb *options.NodeOptionsBuilder) { nb.P2P().SetListenAddresses("/ip4/127.0.0.1/tcp/0") }
	source := start(t, listen, nil)
	target := start(t, listen, func(n *Node) { n.ReplicationFilter = filter })
	addresses, err := target.DB.PeerInfo(ctx)
	if err != nil || len(addresses) == 0 {
		t.Fatalf("peer info %v %v", addresses, err)
	}
	if err := source.DB.AddReplicator(ctx, addresses[:1], options.AddReplicator().SetCollectionNames([]string{"User"})); err != nil {
		t.Fatal(err)
	}
	if err := collectionOf(t, source.DB).(*Collection).SaveManyDocuments(ctx, docs(t, collectionOf(t, source.DB), `{"name": "Alice", "age": 40}`)); err != nil {
		t.Fatal(err)
	}
	want := `{"User":[{"age":40,"name":"Alice"}]}`
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := names(t, target.DB)
		if got == want && filter.calls.Load() >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the target holds %s after %d filter calls; want %s", got, filter.calls.Load(), want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestAFailedAPILeavesNothingOpen(t *testing.T) {
	ctx := context.Background()
	nb := options.Node()
	nb.HTTP().SetAddress("invalid")
	n, err := New(ctx, nb)
	if err != nil {
		t.Fatal(err)
	}
	n.ReplicationFilter = &refuseName{name: "Mallory"}
	if err := n.Start(ctx); err == nil {
		_ = n.Close(ctx)
		t.Fatal("an address it cannot listen on fails Start")
	}
	if n.DB != nil || n.handles != nil {
		t.Fatalf("a failed Start left the database %v and handles %v open", n.DB, n.handles)
	}
}

func TestClosingTheAPIIsNotAnAPIError(t *testing.T) {
	ctx := context.Background()
	nb := options.Node()
	nb.HTTP().SetAddress("127.0.0.1:0")
	n, err := New(ctx, nb)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-n.APIError():
		t.Fatalf("a closed API reported %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

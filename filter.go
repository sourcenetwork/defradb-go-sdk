package defra

/*
#include <stdint.h>
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime/cgo"
	"strconv"
)

// ReplicationFilter decides which replicated documents a node keeps: Go
// Shinzo's replication filter, which Go DefraDB develop does not have.
type ReplicationFilter interface {
	// AllowReplication is asked for each replicated document before any of
	// its blocks are stored, with the collection's name, as Go Shinzo gives
	// it. fields maps each field the document's head
	// commit writes to its value; a value that has not arrived, is encrypted
	// or does not decode is absent, so a filter keeps a document whose
	// deciding field is missing. Return false to drop the document. It is
	// called from several goroutines at once and must not block.
	AllowReplication(ctx context.Context, collectionName string, docID string, fields map[string]any) bool
}

// defraSDKReplicationFilter is the library's replication filter callback for
// a node opened with a ReplicationFilter: context is a cgo.Handle of the
// filter. The library calls it from its database workers and network
// threads, several at once, with the collection's ID and name; the filter is
// given the name.
// A panic, the filter's or a deleted handle's, keeps the document: a panic
// must not cross into the library, and keeping is what a filter that cannot
// decide does.
//
//export defraSDKReplicationFilter
func defraSDKReplicationFilter(context C.uintptr_t, collectionID, collectionName, docID, fieldsJSON *C.char) (keep C.int) {
	defer func() {
		if recover() != nil {
			keep = 1
		}
	}()
	filter, ok := cgo.Handle(context).Value().(ReplicationFilter)
	if !ok {
		return 1
	}
	fields, err := decodeFields(C.GoString(fieldsJSON))
	if err != nil {
		// A filter must keep a document whose deciding field is absent.
		fields = map[string]any{}
	}
	if filter.AllowReplication(backgroundContext, C.GoString(collectionName), C.GoString(docID), fields) {
		return 1
	}
	return 0
}

// backgroundContext is the context a filter is asked in: the library's call
// carries none.
var backgroundContext = context.Background()

// decodeFields reads the fields JSON as Go DefraDB decodes a commit's CBOR
// values: a non-negative integer as uint64, a negative one as int64.
func decodeFields(raw string) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.UseNumber()
	var fields map[string]any
	if err := decoder.Decode(&fields); err != nil {
		return nil, err
	}
	for name, value := range fields {
		fields[name] = numbers(value)
	}
	return fields, nil
}

func numbers(value any) any {
	switch v := value.(type) {
	case json.Number:
		if u, err := v.Int64(); err == nil {
			if u >= 0 {
				return uint64(u)
			}
			return u
		}
		// A uint64 above MaxInt64; the library writes a float as 3.0, never 3,
		// so a whole number here is always an integer.
		if u, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
			return u
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return v.String()
	case []any:
		for i := range v {
			v[i] = numbers(v[i])
		}
		return v
	case map[string]any:
		for k := range v {
			v[k] = numbers(v[k])
		}
		return v
	default:
		return v
	}
}

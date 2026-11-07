package defra

/*
#include <stdlib.h>
#include "defra.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
)

// ListIndexes is every collection's indexes, by collection name.
func (s *store) ListIndexes(
	ctx context.Context,
	opts ...options.Enumerable[options.ListIndexesOptions],
) (map[client.CollectionName][]client.ListIndexesResult, error) {
	if err := s.outsideTxn("ListIndexes"); err != nil {
		return nil, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	raw, err := call[map[client.CollectionName][]json.RawMessage](C.list_all_indexes(s.ptr, a.opt(did(o.Identity))), "list_indexes")
	if err != nil {
		return nil, err
	}
	out := make(map[client.CollectionName][]client.ListIndexesResult, len(raw))
	for name, entries := range raw {
		if out[name], err = indexResults(entries); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ListAllEncryptedIndexes is every collection's encrypted indexes, by
// collection name.
func (s *store) ListAllEncryptedIndexes(
	ctx context.Context,
	opts ...options.Enumerable[options.ListAllEncryptedIndexesOptions],
) (map[client.CollectionName][]client.EncryptedIndexDescription, error) {
	if err := s.outsideTxn("ListAllEncryptedIndexes"); err != nil {
		return nil, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	return call[map[client.CollectionName][]client.EncryptedIndexDescription](
		C.list_all_encrypted_indexes(s.ptr, a.opt(did(o.Identity))), "list_all_encrypted_indexes",
	)
}

// NewIndex adds the ordered index req describes. The library builds ordered
// indexes only, so a vector index is not supported.
func (c *Collection) NewIndex(
	ctx context.Context,
	req client.NewIndexRequest,
	opts ...options.Enumerable[options.NewCollectionIndexOptions],
) (client.IndexDescription, error) {
	if req.Vector != nil {
		return client.IndexDescription{}, errNotSupported("NewIndex of a vector index")
	}
	if err := c.st.outsideTxn("NewIndex"); err != nil {
		return client.IndexDescription{}, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	input, err := a.json(map[string]any{"Name": req.Name, "Fields": req.Fields, "Unique": req.Unique})
	if err != nil {
		return client.IndexDescription{}, err
	}
	return call[client.IndexDescription](
		C.create_index(c.st.ptr, a.opt(did(o.Identity)), a.s(c.Name()), input), "new_index",
	)
}

// DeleteIndex drops the index named indexName.
func (c *Collection) DeleteIndex(
	ctx context.Context,
	indexName string,
	opts ...options.Enumerable[options.DeleteCollectionIndexOptions],
) error {
	if err := c.st.outsideTxn("DeleteIndex"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	_, err := result(C.delete_index(c.st.ptr, a.opt(did(o.Identity)), a.s(c.Name()), a.s(indexName)), "delete_index")
	return err
}

// ListIndexes is the collection's indexes.
func (c *Collection) ListIndexes(
	ctx context.Context,
	opts ...options.Enumerable[options.ListCollectionIndexesOptions],
) ([]client.ListIndexesResult, error) {
	if err := c.st.outsideTxn("ListIndexes"); err != nil {
		return nil, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	raw, err := call[[]json.RawMessage](C.get_indexes(c.st.ptr, a.opt(did(o.Identity)), a.s(c.Name())), "list_indexes")
	if err != nil {
		return nil, err
	}
	return indexResults(raw)
}

// NewEncryptedIndex adds the encrypted index desc describes.
func (c *Collection) NewEncryptedIndex(
	ctx context.Context,
	desc client.EncryptedIndexDescription,
	opts ...options.Enumerable[options.NewEncryptedIndexOptions],
) (client.EncryptedIndexDescription, error) {
	if err := c.st.outsideTxn("NewEncryptedIndex"); err != nil {
		return client.EncryptedIndexDescription{}, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	return call[client.EncryptedIndexDescription](
		C.add_encrypted_index(c.st.ptr, a.opt(did(o.Identity)), a.s(c.Name()), a.s(desc.FieldName)), "new_encrypted_index",
	)
}

// DeleteEncryptedIndex drops the encrypted index on fieldName.
func (c *Collection) DeleteEncryptedIndex(
	ctx context.Context,
	fieldName string,
	opts ...options.Enumerable[options.DeleteEncryptedIndexOptions],
) error {
	if err := c.st.outsideTxn("DeleteEncryptedIndex"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	_, err := result(C.delete_encrypted_index(c.st.ptr, a.opt(did(o.Identity)), a.s(c.Name()), a.s(fieldName)), "delete_encrypted_index")
	return err
}

// ListEncryptedIndexes is the collection's encrypted indexes.
func (c *Collection) ListEncryptedIndexes(
	ctx context.Context,
	opts ...options.Enumerable[options.ListCollectionEncryptedIndexesOptions],
) ([]client.EncryptedIndexDescription, error) {
	if err := c.st.outsideTxn("ListEncryptedIndexes"); err != nil {
		return nil, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	return call[[]client.EncryptedIndexDescription](
		C.list_encrypted_indexes(c.st.ptr, a.opt(did(o.Identity)), a.s(c.Name())), "list_encrypted_indexes",
	)
}

// indexResults reads the library's index list, which holds each index's
// description fields beside its collection name and execution.
func indexResults(entries []json.RawMessage) ([]client.ListIndexesResult, error) {
	out := make([]client.ListIndexesResult, len(entries))
	for i, entry := range entries {
		if err := json.Unmarshal(entry, &out[i].Description); err != nil {
			return nil, fmt.Errorf("defra: list_indexes: %w", err)
		}
		var rest struct {
			CollectionName string
			Execution      client.ActionExecution
		}
		if err := json.Unmarshal(entry, &rest); err != nil {
			return nil, fmt.Errorf("defra: list_indexes: %w", err)
		}
		out[i].CollectionName, out[i].Execution = rest.CollectionName, rest.Execution
	}
	return out, nil
}

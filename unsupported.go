package defra

// Go DefraDB client methods the Rust SDK does not offer yet: each returns
// ErrNotSupported.

import (
	"context"
	"io"
)

func (d *DB) ExportDocKVs(
	ctx context.Context,
	collectionName string,
	docIDs []string,
	w io.Writer,
	datastoreOnly bool,
) (_0 int, err error) {
	err = errNotSupported("ExportDocKVs")
	return
}

func (d *DB) ExportFieldMapping(ctx context.Context, collectionName string) (_0 []byte, err error) {
	err = errNotSupported("ExportFieldMapping")
	return
}

func (d *DB) ImportRawKVs(ctx context.Context, r io.Reader) (_0 int, err error) {
	err = errNotSupported("ImportRawKVs")
	return
}

func (d *DB) ImportRawKVsWithMapping(ctx context.Context, r io.Reader, mappingJSON []byte) (_0 int, err error) {
	err = errNotSupported("ImportRawKVsWithMapping")
	return
}

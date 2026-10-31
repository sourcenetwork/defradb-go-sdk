# defradb-go-sdk

A Go SDK for [defradb.rs](https://github.com/sourcenetwork/defradb.rs). It opens a
Rust DefraDB node in your Go process through defradb.rs's C ABI (`crates/ffi`) and
offers the methods of Go DefraDB's `client` package at Go Shinzo's
`shinzo-develop`, with the same names and argument types, so a Shinzo program
switches to it mostly by changing its import.

```go
import defra "github.com/sourcenetwork/defradb-go-sdk"
```

## The FFI build it needs

The SDK links against the `ffi` library and header of one defradb.rs commit. That
commit is pinned in `DEFRADB_RS_COMMIT`; CI builds exactly that one. Build it the
same way locally:

```bash
git clone https://github.com/sourcenetwork/defradb.rs
cd defradb.rs
git checkout "$(cat ../defradb-go-sdk/DEFRADB_RS_COMMIT)"
cargo build -p ffi --profile super-dev
cbindgen --config crates/ffi/cbindgen.toml --crate ffi --output crates/ffi/defra.h
```

Then point cgo at that checkout (`DEFRADB_RS` is its path):

```bash
export CGO_CFLAGS="-I$DEFRADB_RS/crates/ffi"
export CGO_LDFLAGS="-L$DEFRADB_RS/target/super-dev -Wl,-rpath,$DEFRADB_RS/target/super-dev"
go test ./...
```

The library is named `libffi`, so `-L` and the rpath must name the defradb.rs
target directory ahead of the system's own libffi. Building the FFI needs Rust,
`cbindgen` and `protoc` (see defradb.rs's `CLAUDE.md`, Build Dependencies).

## Moving the pin

The C ABI changes with defradb.rs, so the SDK and its pin move together: update
`DEFRADB_RS_COMMIT` to the new defradb.rs commit in the same change as the Go
code that uses its new or changed functions, and let CI build and test the pair.

## What is not supported

The Go DefraDB client methods the SDK does not offer yet are in `unsupported.go`;
each returns an error that names it and wraps `ErrNotSupported`.

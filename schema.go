package defra

/*
#include <stdlib.h>
#include "defra.h"
*/
import "C"

import (
	"context"
	"encoding/json"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/immutable"
	lensmodel "github.com/sourcenetwork/lens/host-go/config/model"
)

// PatchCollection applies patch, a JSON patch over the collection
// definitions. The library applies a patch without a migration only.
func (s *store) PatchCollection(
	ctx context.Context,
	patch string,
	migration immutable.Option[lensmodel.Lens],
	opts ...options.Enumerable[options.PatchCollectionOptions],
) error {
	if migration.HasValue() {
		return errNotSupported("PatchCollection with a migration")
	}
	if err := s.outsideTxn("PatchCollection"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	_, err := result(C.patch_collection(s.ptr, a.opt(did(o.Identity)), nil, a.s(patch)), "patch_collection")
	return err
}

// DeleteCollection deletes the collections names names, or with ActiveOnly
// only their active versions.
func (s *store) DeleteCollection(
	ctx context.Context,
	names []string,
	opts ...options.Enumerable[options.DeleteCollectionOptions],
) error {
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	namesJSON, err := a.json(names)
	if err != nil {
		return err
	}
	if s.txn != "" {
		_, err = result(C.delete_collections_in_txn(
			s.ptr, a.s(s.txn), a.opt(did(o.Identity)), namesJSON, C.bool(o.ActiveOnly),
		), "delete_collection")
	} else {
		_, err = result(C.delete_collections(s.ptr, a.opt(did(o.Identity)), namesJSON, C.bool(o.ActiveOnly)), "delete_collection")
	}
	return err
}

// SetActiveCollectionVersion makes versionID its collection's active
// version.
func (s *store) SetActiveCollectionVersion(
	ctx context.Context,
	versionID string,
	opts ...options.Enumerable[options.SetActiveCollectionVersionOptions],
) error {
	if err := s.outsideTxn("SetActiveCollectionVersion"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	_, err := result(C.set_active_collection_version(s.ptr, a.opt(did(o.Identity)), a.s(versionID)), "set_active_collection_version")
	return err
}

// RefreshViews refreshes every view, or the one CollectionName names. The
// library selects views by name only.
func (s *store) RefreshViews(ctx context.Context, opts ...options.Enumerable[options.RefreshViewsOptions]) error {
	o := newOptions(opts...)
	if o.VersionID.HasValue() || o.CollectionID.HasValue() || o.CollectionSetID.HasValue() || o.GetInactive.HasValue() {
		return errNotSupported("RefreshViews by anything but CollectionName")
	}
	if err := s.outsideTxn("RefreshViews"); err != nil {
		return err
	}
	var a cargs
	defer a.free()
	var names *C.char
	if o.CollectionName.HasValue() {
		var err error
		if names, err = a.json(map[string][]string{"Names": {o.CollectionName.Value()}}); err != nil {
			return err
		}
	}
	_, err := result(C.refresh_views(s.ptr, a.opt(did(o.Identity)), names), "refresh_views")
	return err
}

// SetMigration sets the lens migration between the versions config names
// and returns the migration's ID.
func (s *store) SetMigration(
	ctx context.Context,
	config client.LensConfig,
	opts ...options.Enumerable[options.SetMigrationOptions],
) (string, error) {
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	if config.Lens.Lenses == nil {
		config.Lens.Lenses = []lensmodel.LensModule{}
	}
	configJSON, err := a.json(config)
	if err != nil {
		return "", err
	}
	if s.txn != "" {
		return result(C.set_migration_in_txn(s.ptr, a.s(s.txn), a.opt(did(o.Identity)), configJSON), "set_migration")
	}
	return result(C.set_migration(s.ptr, a.opt(did(o.Identity)), configJSON), "set_migration")
}

// ListLenses is every stored lens, by ID.
func (s *store) ListLenses(ctx context.Context, opts ...options.Enumerable[options.ListLensesOptions]) (map[string]lensmodel.Lens, error) {
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	var r C.struct_FfiResult
	if s.txn != "" {
		r = C.lens_list_in_txn(s.ptr, a.s(s.txn), a.opt(did(o.Identity)))
	} else {
		r = C.lens_list(s.ptr, a.opt(did(o.Identity)))
	}
	raw, err := call[map[string]json.RawMessage](r, "list_lenses")
	if err != nil {
		return nil, err
	}
	lenses := make(map[string]lensmodel.Lens, len(raw))
	for id, value := range raw {
		lens, err := decodeLens(value)
		if err != nil {
			return nil, err
		}
		lenses[id] = lens
	}
	return lenses, nil
}

// decodeLens reads a stored lens: a whole lens, or one module as the
// library may give it.
func decodeLens(raw json.RawMessage) (lensmodel.Lens, error) {
	var lens lensmodel.Lens
	if err := json.Unmarshal(raw, &lens); err == nil && len(lens.Lenses) > 0 {
		return lens, nil
	}
	var module lensmodel.LensModule
	if err := json.Unmarshal(raw, &module); err != nil {
		return lensmodel.Lens{}, err
	}
	return lensmodel.Lens{Lenses: []lensmodel.LensModule{module}}, nil
}

// BasicImport imports the documents of the backup at filepath.
func (s *store) BasicImport(ctx context.Context, filepath string) error {
	if err := s.outsideTxn("BasicImport"); err != nil {
		return err
	}
	var a cargs
	defer a.free()
	_, err := result(C.basic_import(s.ptr, a.s(filepath)), "basic_import")
	return err
}

// BasicExport writes a backup of the database to filepath.
func (s *store) BasicExport(ctx context.Context, filepath string, opts ...options.Enumerable[options.BasicExportOptions]) error {
	if err := s.outsideTxn("BasicExport"); err != nil {
		return err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	config, err := a.json(client.BackupConfig{
		Filepath:    filepath,
		Format:      o.Format,
		Pretty:      o.Pretty,
		Collections: o.Collections,
	})
	if err != nil {
		return err
	}
	_, err = result(C.basic_export(s.ptr, config), "basic_export")
	return err
}

// ListActions is the actions in progress or ended with an error.
func (s *store) ListActions(ctx context.Context, opts ...options.Enumerable[options.ListActionsOptions]) ([]client.ActionExecution, error) {
	if err := s.outsideTxn("ListActions"); err != nil {
		return nil, err
	}
	o := newOptions(opts...)
	var a cargs
	defer a.free()
	return call[[]client.ActionExecution](C.list_actions(s.ptr, a.opt(did(o.Identity))), "list_actions")
}

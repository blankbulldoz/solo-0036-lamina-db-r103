package engine

import (
	"fmt"
	"lamina/internal/catalog"
	"lamina/internal/identity"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
	"sync"
)

type DB struct {
	mu           sync.RWMutex
	dir          string
	catalog      *catalog.Catalog
	store        *versions.Store
	indexes      *index.Registry
	wal          *journal.WAL
	sequence     uint64
	horizon      uint64
	provenance   identity.Provenance
	format       int
	transactions map[string]*Transaction
	closed       bool
	commits      uint64
	conflicts    uint64
	queries      uint64
}

// RootLineage labels an instance created from nothing.
const RootLineage = "root"

func Open(dir string) (*DB, error) {
	return openDir(dir, openOptions{})
}

type openOptions struct {
	// allowLegacy admits identity-less artifacts for the one-time upgrade.
	allowLegacy bool
	// locked is an already-held directory lock; the WAL takes ownership.
	locked *os.File
	// noTruncate leaves an incomplete final frame untouched.
	noTruncate bool
	// expectLegacy requires both artifacts to be legacy; the upgrade uses it
	// so a partially converted directory is never validated as if it were
	// consistent.
	expectLegacy bool
}

// openDir is the one admission path: it validates the checkpoint, the journal
// header, their shared identity, their sequence relationship and every
// replayed record before publishing a database. When allowLegacy is set, the
// identity-less legacy formats are admitted for the one-time upgrade only.
func openDir(dir string, opt openOptions) (*DB, error) {
	allowLegacy := opt.allowLegacy
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	checkpointPath := filepath.Join(absolute, "checkpoint.lmi")
	image, legacyImage, err := snapshot.ReadCompat(checkpointPath)
	missing := os.IsNotExist(err)
	if err != nil && !missing {
		return nil, fmt.Errorf("checkpoint: %w", err)
	}
	if legacyImage && !allowLegacy {
		return nil, fmt.Errorf("checkpoint has no instance identity; run `laminad upgrade -data %s` before serving", absolute)
	}
	if missing && !allowLegacy && fileExists(filepath.Join(absolute, journal.JournalFile)) && fileExists(filepath.Join(absolute, "checkpoint.v1.lmi")) {
		return nil, fmt.Errorf("journal without checkpoint; the directory is partially converted, run `laminad upgrade -data %s` to restore the checkpoint", absolute)
	}
	if !legacyImage && !missing && opt.expectLegacy {
		return nil, fmt.Errorf("checkpoint is already identity-bearing; not a pending upgrade")
	}
	if !missing {
		if _, err = os.Stat(filepath.Join(absolute, journal.JournalFile)); err != nil {
			return nil, fmt.Errorf("checkpoint without journal; use validated restore")
		}
	}
	if missing && allowLegacy {
		return nil, fmt.Errorf("no checkpoint and no journal")
	}
	var create, expect *identity.Provenance
	if !missing {
		create, expect = image.Provenance, image.Provenance
	} else {
		id, idErr := identity.New()
		if idErr != nil {
			return nil, idErr
		}
		create = &identity.Provenance{Instance: id, Lineage: RootLineage}
	}
	w, err := journal.Open(absolute, journal.Options{Base: image.Sequence, Expect: expect, Create: create, AllowLegacy: allowLegacy, Locked: opt.locked, NoTruncate: opt.noTruncate})
	if err != nil {
		return nil, err
	}
	if missing && w.HasIdentity && (w.Adoption != nil || fileExists(filepath.Join(absolute, "checkpoint.v1.lmi"))) {
		// A converted directory always keeps its adopted image. Losing the
		// serving checkpoint while that evidence remains means the upgrade
		// was interrupted; it must not silently serve as a replayed journal.
		w.Close()
		return nil, fmt.Errorf("journal without checkpoint; the directory is partially converted, run `laminad upgrade -data %s` to restore the checkpoint", absolute)
	}
	if w.Legacy && !allowLegacy {
		w.Close()
		return nil, fmt.Errorf("journal has no instance identity; run `laminad upgrade -data %s` before serving", absolute)
	}
	if w.Adoption != nil {
		if err = verifyRollbackMaterial(absolute, *w.Adoption); err != nil {
			w.Close()
			return nil, err
		}
	}
	if opt.expectLegacy && !w.Legacy && w.Adoption == nil {
		w.Close()
		return nil, fmt.Errorf("journal is already identity-bearing; not a pending upgrade")
	}
	if !missing && image.Sequence < w.Base {
		w.Close()
		return nil, fmt.Errorf("checkpoint sequence %d is older than journal base %d; the missing history cannot be reconstructed", image.Sequence, w.Base)
	}
	d := &DB{dir: absolute, catalog: catalog.New(), store: versions.New(), wal: w, transactions: map[string]*Transaction{}, format: identity.CurrentFormat}
	if !missing {
		d.catalog = image.Catalog
		d.store = image.Store
		d.sequence = image.Sequence
		d.horizon = image.Horizon
	}
	if !missing && !legacyImage {
		d.provenance = *image.Provenance
	}
	if w.HasIdentity {
		d.provenance = w.Provenance
	}
	if d.provenance.Instance == "" {
		// Legacy upgrade validation only: this database is never served.
		d.provenance = identity.Provenance{Instance: "00000000000000000000000000000000", Lineage: identity.Lineage(identity.LegacyFormat, identity.CurrentFormat, "validation")}
	}
	last := w.Base + uint64(len(w.Records))
	if d.sequence > last {
		w.Close()
		return nil, fmt.Errorf("checkpoint ahead of journal")
	}
	for _, r := range w.Records {
		if r.Sequence <= d.sequence {
			continue
		}
		if err = d.apply(r); err != nil {
			w.Close()
			return nil, fmt.Errorf("replay sequence %d: %w", r.Sequence, err)
		}
	}
	d.indexes = index.Build(d.catalog, d.store)
	for _, relation := range d.catalog.List() {
		if err = d.unique(relation.Name, nil); err != nil {
			w.Close()
			return nil, fmt.Errorf("recovered constraints: %w", err)
		}
	}
	return d, nil
}

// verifyRollbackMaterial checks that the preserved legacy image still pairs
// with the journal prefix the adoption record describes. A present but
// different image is refused; a missing one is reported, not silently
// ignored.
func verifyRollbackMaterial(dir string, a journal.Adoption) error {
	if !a.LegacyCheckpoint {
		return nil
	}
	path := filepath.Join(dir, a.CheckpointBackup)
	if a.CheckpointBackup == "" || filepath.Base(a.CheckpointBackup) != a.CheckpointBackup {
		return fmt.Errorf("adoption record names an invalid rollback image")
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	digest, err := journal.DigestFile(path)
	if err != nil {
		return err
	}
	if a.CheckpointDigest == "" || digest != a.CheckpointDigest {
		return fmt.Errorf("preserved legacy checkpoint %s does not match the adoption record", a.CheckpointBackup)
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func (d *DB) usable() error {
	if d.closed {
		return fault("unavailable", "database closed")
	}
	if d.wal.Poisoned {
		return fault("unavailable", "journal write outcome uncertain; restart required")
	}
	return nil
}
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.transactions = map[string]*Transaction{}
	return d.wal.Close()
}

// Identity describes what this directory claims to be and where its history
// came from. It is copied, not computed per call, so a checkpoint cannot drift
// from the journal under a live process.
type Identity struct {
	Provenance       identity.Provenance `json:"provenance"`
	Format           int                 `json:"format"`
	ReadableFormats  string              `json:"readable_formats"`
	Formats          []identity.Version  `json:"formats"`
	JournalBase      uint64              `json:"journal_base"`
	AdoptedLegacy    bool                `json:"adopted_legacy"`
	LegacyBytes      int64               `json:"legacy_bytes,omitempty"`
	LegacyCheckpoint string              `json:"legacy_checkpoint,omitempty"`
	CheckpointSeq    uint64              `json:"checkpoint_sequence"`
}

func (d *DB) Identity() Identity {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := Identity{Provenance: d.provenance, Format: d.format, ReadableFormats: identity.CompatibleRange(), Formats: identity.Versions(), JournalBase: d.wal.Base, CheckpointSeq: d.sequence}
	if d.wal.Adoption != nil {
		out.AdoptedLegacy = true
		out.LegacyBytes = d.wal.Adoption.LegacyBytes
		out.LegacyCheckpoint = d.wal.Adoption.CheckpointBackup
	}
	return out
}

func (d *DB) publish(r journal.Record) error {
	r.Sequence = d.sequence + 1
	r.Format = d.format
	if err := d.wal.Append(r); err != nil {
		return fault("unavailable", "durable commit failed: %s", err)
	}
	if err := d.apply(r); err != nil {
		d.wal.Poisoned = true
		return fault("unavailable", "durable record cannot be applied: %s", err)
	}
	if len(r.Writes) > 0 {
		d.indexes.Apply(d.catalog, r.Writes)
	}
	return nil
}
func (d *DB) Relations() ([]catalog.Relation, []catalog.Index, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, nil, err
	}
	return d.catalog.List(), d.catalog.IndexList(), nil
}

package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"lamina/internal/identity"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
)

func (d *DB) image() snapshot.Image {
	p := d.provenance
	return snapshot.Image{Format: identity.CurrentFormat, Sequence: d.sequence, Horizon: d.horizon, Provenance: &p, Catalog: d.catalog.Clone(), Store: d.store.Clone()}
}

func (d *DB) Checkpoint() (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return 0, err
	}
	if err := snapshot.Write(filepath.Join(d.dir, "checkpoint.lmi"), d.image()); err != nil {
		return 0, fault("unavailable", "checkpoint: %s", err)
	}
	return d.sequence, nil
}
func (d *DB) Backup() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	b, err := snapshot.Encode(d.image())
	if err != nil {
		return nil, err
	}
	if len(b) > snapshot.MaxImage {
		return nil, fault("capacity", "backup exceeds 128MiB")
	}
	return b, nil
}

// Restore installs a backup into a new directory as an independent instance.
// Identity-bearing backups keep their origin and lineage but receive a fresh
// instance id and name the source as parent; identity-less legacy backups are
// adopted with a conversion lineage. Either way the restored directory states
// what it inherited and never claims the source's later commits.
func Restore(dir string, data []byte) error {
	image, legacy, err := snapshot.DecodeCompat(data)
	if err != nil {
		return err
	}
	if err = os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("restore requires a new directory: %w", err)
	}
	installed := false
	defer func() {
		if !installed {
			os.Remove(filepath.Join(dir, "checkpoint.lmi"))
			os.Remove(filepath.Join(dir, journal.JournalFile))
			os.Remove(filepath.Join(dir, journal.IdentityFile))
			os.Remove(filepath.Join(dir, "lamina.lock"))
			os.Remove(dir)
		}
	}()
	id, err := identity.New()
	if err != nil {
		return err
	}
	if legacy {
		image.Format = identity.CurrentFormat
		digest := imageDigest(data)
		image.Provenance = &identity.Provenance{Instance: id, Lineage: identity.Lineage(identity.LegacyFormat, identity.CurrentFormat, digest[:32])}
	} else {
		p := identity.Inherited(id, *image.Provenance)
		image.Provenance = &p
	}
	if err = image.Provenance.Check(); err != nil {
		return err
	}
	w, err := journal.Open(dir, journal.Options{Base: image.Sequence, Create: image.Provenance})
	if err != nil {
		return err
	}
	if err = snapshot.Write(filepath.Join(dir, "checkpoint.lmi"), image); err != nil {
		w.Close()
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	installed = true
	return nil
}
func (d *DB) Vacuum(requested uint64) (versions.VacuumResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return versions.VacuumResult{}, err
	}
	if requested > d.sequence || requested < d.horizon {
		return versions.VacuumResult{}, fault("invalid", "vacuum horizon outside retained interval")
	}
	horizon := requested
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
		}
	}
	beforeKeys, beforeVersions := d.store.Count()
	_ = beforeKeys
	if err := d.publish(journal.Record{Kind: "vacuum", Horizon: horizon}); err != nil {
		return versions.VacuumResult{}, err
	}
	_, remaining := d.store.Count()
	d.indexes = index.Build(d.catalog, d.store)
	return versions.VacuumResult{Horizon: horizon, Removed: beforeVersions - remaining, Retained: remaining, Pinned: horizon < requested}, nil
}

func imageDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

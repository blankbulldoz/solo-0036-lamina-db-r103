// Package identity defines the persistent identity of a LaminaDB instance and
// the format-compatibility contract shared by every durable artifact.
//
// A database directory is no longer "whatever files happen to share a
// sequence number": the instance id and the format lineage are written into
// the journal header and into every checkpoint/backup image, and both are
// checked against each other before any record is replayed.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
)

// Format lineage. An upgrade creates a new lineage; it does not relabel the
// old one. Files produced by different lineages can be recognised and refused
// instead of being stitched together by sequence number alone.
const (
	// LegacyFormat is the pre-identity on-disk format (LMW1 journal header,
	// payload field "format":1, image envelope "format":1). It carries no
	// instance id and cannot prove provenance.
	LegacyFormat = 1
	// CurrentFormat is the identity-bearing format (LMW2, payload "format":2,
	// image envelope format 2).
	CurrentFormat = 2
	// ReadableMin/ReadableMax bound the payload formats this binary can
	// interpret after an artifact has been admitted.
	ReadableMin = 2
	ReadableMax = 2

	// Component names identify which artifact a header belongs to.
	ComponentJournal    = "journal"
	ComponentCheckpoint = "checkpoint"
	ComponentBackup     = "backup"
)

// Version describes one readable payload format. It is published in
// /stats and printed by the migrate tool so operators can state the range.
type Version struct {
	Format int    `json:"format"`
	Layout string `json:"layout"`
}

var versions = []Version{
	{Format: 1, Layout: "legacy-identityless"},
	{Format: 2, Layout: "identity-bearing"},
}

func Versions() []Version {
	out := make([]Version, len(versions))
	copy(out, versions)
	return out
}

// CompatibleFormat reports whether payload format f can be interpreted.
func CompatibleFormat(f int) bool { return f >= ReadableMin && f <= ReadableMax }

func CompatibleRange() string { return fmt.Sprintf("%d..%d", ReadableMin, ReadableMax) }

var idPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// ValidateID accepts the canonical 128-bit lowercase hex instance id.
func ValidateID(id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("instance id must be 32 lowercase hex characters")
	}
	return nil
}

// New returns a fresh random instance id.
func New() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

// Provenance is the identity block embedded in the journal header and in each
// image payload. Lineage names the format conversion that produced the file;
// Origin names the instance the data was originally written by; Parent names
// the instance this directory inherited its data from (empty for a root
// instance). A restored or copied instance therefore states what it inherited
// instead of pretending to be the source.
type Provenance struct {
	Instance string `json:"instance"`
	Origin   string `json:"origin,omitempty"`
	Parent   string `json:"parent,omitempty"`
	Lineage  string `json:"lineage"`
}

// Root returns the provenance of an instance created by an upgrade of legacy
// material. The upgraded directory keeps writing under a new instance id but
// the issuer and lineage record where its history came from.
func Root(instance, issuer, lineage string) Provenance {
	return Provenance{Instance: instance, Origin: issuer, Lineage: lineage}
}

// Inherited returns the provenance of a restored instance: it inherits from
// source's origin and lineage, records source as parent, and may not claim to
// continue the source's commits.
func Inherited(instance string, source Provenance) Provenance {
	origin := source.Origin
	if origin == "" {
		origin = source.Instance
	}
	return Provenance{Instance: instance, Origin: origin, Parent: source.Instance, Lineage: source.Lineage}
}

// Check performs the consistency checks shared by journal and image readers.
func (p Provenance) Check() error {
	if err := ValidateID(p.Instance); err != nil {
		return err
	}
	if p.Origin != "" {
		if err := ValidateID(p.Origin); err != nil {
			return fmt.Errorf("origin: %w", err)
		}
	}
	if p.Parent != "" {
		if err := ValidateID(p.Parent); err != nil {
			return fmt.Errorf("parent: %w", err)
		}
		if p.Parent == p.Instance {
			return fmt.Errorf("instance cannot be its own parent")
		}
	}
	if p.Lineage == "" || len(p.Lineage) > 128 {
		return fmt.Errorf("lineage must be 1..128 characters")
	}
	return nil
}

// CheckSame reports whether a journal and an image carry one identity and an
// overlapping format contract. It is the gate that stops equal sequence
// numbers from different sources from being paired into one database.
func CheckSame(journal, image Provenance) error {
	if journal.Instance != image.Instance {
		return fmt.Errorf("journal belongs to instance %s but checkpoint to %s", journal.Instance, image.Instance)
	}
	if journal.Lineage != image.Lineage {
		return fmt.Errorf("journal lineage %q does not match checkpoint lineage %q", journal.Lineage, image.Lineage)
	}
	return nil
}

// Lineage builds a deterministic, human-readable conversion label including a
// fingerprint of the material it was applied to.
func Lineage(from, to int, fingerprint string) string {
	return fmt.Sprintf("legacy-v%d-to-v%d:%s", from, to, fingerprint)
}

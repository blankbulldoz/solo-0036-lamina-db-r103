package engine

import (
	"fmt"
	"lamina/internal/identity"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"os"
	"path/filepath"
)

// RecoverReport is the read-only diagnosis of a data directory. It never
// mutates bytes and never decides silently: it states which artifacts are
// present, which identity they claim, what a serving open would decide, and
// where the readable history ends.
type RecoverReport struct {
	Directory     string               `json:"directory"`
	State         string               `json:"state"`
	ServingOK     bool                 `json:"serving_ok"`
	ServingError  string               `json:"serving_error,omitempty"`
	Checkpoint    *ArtifactReport      `json:"checkpoint,omitempty"`
	RollbackImage *ArtifactReport      `json:"rollback_image,omitempty"`
	Journal       *JournalReport       `json:"journal,omitempty"`
	Adoption      *journal.Adoption    `json:"adoption,omitempty"`
	Identity      *identity.Provenance `json:"identity,omitempty"`
	Keys          int                  `json:"readable_keys"`
	Versions      int                  `json:"readable_versions"`
	Sequence      uint64               `json:"readable_sequence"`
	RecoveredTail int64                `json:"recovered_tail_bytes"`
	Advice        string               `json:"advice,omitempty"`
}

type ArtifactReport struct {
	Path     string `json:"path"`
	Format   int    `json:"format"`
	Legacy   bool   `json:"legacy"`
	Digest   string `json:"sha256"`
	Sequence uint64 `json:"sequence"`
	Instance string `json:"instance,omitempty"`
	Lineage  string `json:"lineage,omitempty"`
	Valid    bool   `json:"valid"`
	Error    string `json:"error,omitempty"`
}

type JournalReport struct {
	Path          string `json:"path"`
	Format        int    `json:"format"`
	Legacy        bool   `json:"legacy"`
	Base          uint64 `json:"base"`
	Records       int    `json:"records"`
	Bytes         int64  `json:"bytes"`
	Instance      string `json:"instance,omitempty"`
	Lineage       string `json:"lineage,omitempty"`
	RecoveredTail int64  `json:"recovered_tail_bytes"`
	Depth         int    `json:"depth"`
	Adopted       bool   `json:"adopted"`
	Layout        string `json:"layout"`
	PrefixBytes   int64  `json:"adopted_prefix_bytes,omitempty"`
	SuffixRecords int    `json:"identity_records,omitempty"`
}

// Recover inspects a directory without taking the write lock, without
// truncating, and without installing anything. It reads the journal through
// the same parser the server uses (allowing legacy input) and reports what
// would be served or why nothing would be.
func Recover(dir string) (RecoverReport, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return RecoverReport{}, err
	}
	report := RecoverReport{Directory: absolute, Sequence: 0}
	checkpointPath := filepath.Join(absolute, CheckpointName)
	legacyPath := filepath.Join(absolute, "checkpoint.v1.lmi")
	journalPath := filepath.Join(absolute, journal.JournalFile)

	if a, err := journal.ReadAdoption(absolute); err == nil && a != nil {
		report.Adoption = a
		p := a.Provenance
		report.Identity = &p
	} else if err != nil {
		report.State = "unreadable-adoption"
		report.Advice = "the adoption record is unreadable: " + err.Error()
	}

	image, legacy, imageErr := snapshot.ReadCompat(checkpointPath)
	switch {
	case imageErr == nil:
		report.Checkpoint = artifactReport(checkpointPath, image, legacy, "")
	case os.IsNotExist(imageErr):
	default:
		report.Checkpoint = &ArtifactReport{Path: checkpointPath, Valid: false, Error: imageErr.Error()}
	}
	if legacy || imageErr != nil {
		if rollback, rollbackLegacy, err := snapshot.ReadCompat(legacyPath); err == nil {
			report.RollbackImage = artifactReport(legacyPath, rollback, rollbackLegacy, "")
		}
	}

	if f, err := os.Open(journalPath); err == nil {
		f.Close()
		if jr, err := inspectJournal(absolute); err == nil {
			report.Journal = &jr
		}
	}

	if err := classifyLoad(absolute, &report); err != nil {
		report.ServingError = err.Error()
	}
	if report.ServingOK {
		report.State = "serving-identity-bearing"
		if report.Adoption != nil {
			report.State = "serving-upgraded"
		}
		report.Advice = "the directory opens through the normal admission path; no conversion is pending"
	} else if report.Checkpoint != nil && report.Checkpoint.Legacy && report.Journal != nil && report.Journal.Legacy {
		report.State = "legacy"
		report.Advice = fmt.Sprintf("readable legacy history ends at sequence %d; run `laminad upgrade -data %s` (add -cut-tail if the reported tail must be discarded)", report.Sequence, absolute)
	}
	return report, nil
}

func artifactReport(path string, image snapshot.Image, legacy bool, digest string) *ArtifactReport {
	if digest == "" {
		digest, _ = journal.DigestFile(path)
	}
	out := &ArtifactReport{Path: path, Format: image.Format, Legacy: legacy, Digest: digest, Sequence: image.Sequence, Valid: true}
	if image.Provenance != nil {
		out.Instance = image.Provenance.Instance
		out.Lineage = image.Provenance.Lineage
	}
	return out
}

func inspectJournal(dir string) (JournalReport, error) {
	var out JournalReport
	path := filepath.Join(dir, journal.JournalFile)
	out.Path = path
	format, legacy, err := classifyJournal(path)
	if err != nil {
		return out, err
	}
	out.Format, out.Legacy = format, legacy
	info, err := os.Stat(path)
	if err != nil {
		return out, err
	}
	out.Bytes = info.Size()
	lock, err := journal.LockDir(dir)
	if err != nil {
		return out, fmt.Errorf("directory is in use: %w", err)
	}
	defer lock.Close()
	w, err := journal.Open(dir, journal.Options{Base: ^uint64(0), AllowLegacy: true, NoTruncate: true, Locked: lock})
	if err != nil {
		return out, err
	}
	defer w.Close()
	out.Base = w.Base
	out.Records = len(w.Records)
	out.RecoveredTail = w.RecoveredTail
	out.Depth = len(w.Records)
	if w.HasIdentity {
		out.Instance = w.Provenance.Instance
		out.Lineage = w.Provenance.Lineage
	}
	out.Adopted = w.Adoption != nil
	if w.Adoption != nil {
		out.PrefixBytes = w.Adoption.LegacyBytes
		out.SuffixRecords = len(w.Records) - w.Adoption.LegacyRecords
		out.Layout = "legacy-prefix+identity-suffix"
	} else if w.HasIdentity {
		out.Layout = "identity"
	} else {
		out.Layout = "legacy"
	}
	return out, nil
}

// classifyLoad runs the serving admission path and records its verdict. When
// serving refuses legacy input, the same parser is run in legacy mode so the
// report still states how far the retained history is readable.
func classifyLoad(dir string, report *RecoverReport) error {
	lock, err := journal.LockDir(dir)
	if err != nil {
		return fmt.Errorf("directory is in use: %w", err)
	}
	defer lock.Close()
	d, servingErr := openDir(dir, openOptions{locked: lock, noTruncate: true})
	if servingErr == nil {
		defer d.Close()
		keys, versions := d.store.Count()
		report.ServingOK = true
		report.Keys, report.Versions, report.Sequence, report.RecoveredTail = keys, versions, d.sequence, d.wal.RecoveredTail
		// The report describes the artifacts on disk; the identity it names
		// is the one the serving path actually accepted.
		accepted := d.provenance
		if d.wal.Adoption != nil {
			accepted = d.wal.Adoption.Provenance
		}
		report.Identity = &accepted
		return nil
	}
	report.ServingError = servingErr.Error()
	report.Advice = ""
	legacy, legacyErr := openDir(dir, openOptions{allowLegacy: true, locked: lock, noTruncate: true})
	if legacyErr != nil {
		report.State = "unreadable"
		report.Advice = "the retained history cannot be read past this point: " + legacyErr.Error()
		return nil
	}
	defer legacy.Close()
	keys, versions := legacy.store.Count()
	report.Keys, report.Versions, report.Sequence, report.RecoveredTail = keys, versions, legacy.sequence, legacy.wal.RecoveredTail
	return nil
}

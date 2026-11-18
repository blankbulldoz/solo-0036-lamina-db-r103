package engine

import (
	"fmt"
	"io"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/identity"
	"lamina/internal/journal"
	"lamina/internal/snapshot"
	"lamina/internal/versions"
	"os"
	"path/filepath"
	"time"
)

// CheckpointName is the only checkpoint the serving path reads.
const CheckpointName = "checkpoint.lmi"

// UpgradeReport is the machine-readable account of an in-place conversion.
// It names the identity that now owns the directory, the format contract, the
// exact legacy bytes that were adopted, and the recovery boundary an operator
// can fall back to.
type UpgradeReport struct {
	Status           string              `json:"status"`
	Instance         identity.Provenance `json:"provenance"`
	Format           int                 `json:"format"`
	FromFormat       int                 `json:"from_format"`
	ReadableFormats  string              `json:"readable_formats"`
	CheckpointSeq    uint64              `json:"checkpoint_sequence"`
	Head             uint64              `json:"head"`
	LegacyJournal    journal.Adoption    `json:"legacy_journal"`
	LegacyCheckpoint string              `json:"legacy_checkpoint,omitempty"`
	RetainedPrefix   int64               `json:"retained_legacy_bytes"`
	RecoveredTail    int64               `json:"recovered_tail_bytes"`
	CutTailBytes     int64               `json:"discarded_tail_bytes"`
	Records          int                 `json:"records"`
	Keys             int                 `json:"keys"`
	Versions         int                 `json:"versions"`
	CrossCheck       CrossCheck          `json:"legacy_cross_check"`
	Rollback         RollbackPlan        `json:"rollback"`
}

// RollbackPlan is the executable inverse the operator must be given when the
// upgrade chooses to convert in place. There is no second copy of the data:
// the v1 prefix stays byte-for-byte in place and the v1 checkpoint is moved
// aside into the same directory.
type RollbackPlan struct {
	Executable   bool   `json:"executable"`
	Command      string `json:"command"`
	Checkpoint   string `json:"checkpoint"`
	JournalBytes int64  `json:"journal_prefix_bytes"`
	Note         string `json:"note"`
}

// Upgrade converts a legacy directory in place.
//
// The conversion is staged so that an interruption never leaves a directory
// that both a v1 and a v2 reader would accept:
//
//  1. take the exclusive directory lock (no live owner),
//  2. validate the legacy checkpoint and replay the complete legacy journal
//     with the normal admission rules,
//  3. sync the legacy journal, hash the retained v1 prefix (header + frames),
//  4. move checkpoint.lmi to checkpoint.v1.lmi (rename, directory sync),
//  5. write the v2 checkpoint under the new identity (atomic rename),
//  6. write journal.lmw.id last (atomic rename), which is the commit point,
//  7. verify through the normal serving admission path.
//
// An interruption is always recognisable: with the v1 image renamed away and
// no adoption record the directory does not serve at all; with the adoption
// record present the next run resumes under the identity already committed,
// never under a second identity. No phase leaves two checkpoints that a v1
// and a v2 reader would each accept as the head.
func Upgrade(dir string, now time.Time, cutTail, assumeContinuous bool) (UpgradeReport, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return UpgradeReport{}, err
	}
	lock, err := journal.LockDir(absolute)
	if err != nil {
		return UpgradeReport{}, err
	}
	defer lock.Close()
	report := UpgradeReport{Status: "upgraded", FromFormat: identity.LegacyFormat, Format: identity.CurrentFormat, ReadableFormats: identity.CompatibleRange()}

	checkpointPath := filepath.Join(absolute, CheckpointName)
	legacyCheckpoint := filepath.Join(absolute, "checkpoint.v1.lmi")
	journalPath := filepath.Join(absolute, journal.JournalFile)
	adoptionPath := filepath.Join(absolute, journal.IdentityFile)

	if _, statErr := os.Stat(adoptionPath); statErr == nil {
		// The adoption record is the upgrade commit marker. Its presence
		// means an earlier run got past the point of no return: resume from
		// whatever phase it reached instead of starting over or refusing.
		existing, readErr := journal.ReadAdoption(absolute)
		if readErr != nil {
			return report, fmt.Errorf("unreadable adoption record: %w", readErr)
		}
		if existing == nil {
			return report, fmt.Errorf("identity record exists but cannot be decoded")
		}
		return resumeFromAdoption(absolute, lock, *existing, report)
	}
	image, checkpointLegacy, err := snapshot.ReadCompat(checkpointPath)
	missingCheckpoint := os.IsNotExist(err)
	if err != nil && !missingCheckpoint {
		return report, fmt.Errorf("checkpoint: %w", err)
	}
	_, journalLegacy, journalErr := classifyJournal(journalPath)
	if journalErr != nil {
		return report, journalErr
	}
	if missingCheckpoint {
		// An interrupted run may have moved the legacy image aside before
		// writing the replacement. Restore exactly what was there.
		preserved, preservedLegacy, readErr := snapshot.ReadCompat(legacyCheckpoint)
		if readErr != nil {
			if os.IsNotExist(readErr) {
				return report, fmt.Errorf("no checkpoint; nothing to upgrade")
			}
			return report, fmt.Errorf("preserved legacy checkpoint: %w", readErr)
		}
		if !preservedLegacy {
			return report, fmt.Errorf("preserved %s is not a legacy image", filepath.Base(legacyCheckpoint))
		}
		if !journalLegacy {
			return report, fmt.Errorf("no checkpoint, identity-bearing journal and no adoption record; refusing to guess provenance")
		}
		// Nothing identity-bearing was installed. Put the legacy image back
		// and continue from the exact pre-upgrade state.
		if err = os.Rename(legacyCheckpoint, checkpointPath); err != nil {
			return report, fmt.Errorf("restore interrupted legacy checkpoint: %w", err)
		}
		if err = syncDir(absolute); err != nil {
			return report, err
		}
		image = preserved
	} else {
		if !checkpointLegacy && !journalLegacy {
			opened, err := openDir(absolute, openOptions{locked: lock, noTruncate: true})
			if err != nil {
				return report, fmt.Errorf("directory already identity-bearing but does not open: %w", err)
			}
			p := opened.provenance
			keys, versions := opened.store.Count()
			report.Status = "already-upgraded"
			report.Instance = p
			report.CheckpointSeq, report.Head = opened.sequence, opened.sequence
			report.Keys, report.Versions = keys, versions
			opened.Close()
			return report, nil
		}
		if !checkpointLegacy && journalLegacy {
			return report, fmt.Errorf("checkpoint is identity-bearing but journal is legacy; refusing to pair artifacts that cannot prove a shared identity")
		}
	}

	// Full legacy validation happens through the same admission path that
	// serving uses; nothing is published from it.
	d, err := openDir(absolute, openOptions{allowLegacy: true, locked: lock, noTruncate: true, expectLegacy: true})
	if err != nil {
		return report, err
	}
	report.CheckpointSeq = d.sequence
	report.Head = d.sequence
	report.Records = len(d.wal.Records)
	report.Keys, report.Versions = d.store.Count()
	report.RecoveredTail = d.wal.RecoveredTail
	if d.wal.Adoption != nil && report.RecoveredTail != 0 {
		// A torn frame after the adopted prefix is not legacy material.
		// Refuse instead of cutting identity-bearing bytes.
		if d.wal.RecoveredTail > 0 {
			d.wal.Close()
			return report, fmt.Errorf("journal has an incomplete identity-bearing tail; restore from backup or inspect with `laminad recover`")
		}
	}
	report.CrossCheck = reconcile(d, image)
	if report.CrossCheck.Result == "mismatch" {
		d.wal.Close()
		return report, fmt.Errorf("legacy checkpoint and journal disagree at sequence %d (%d differences: %s); they were not proven to belong together, refusing to assign them one identity", d.sequence, len(report.CrossCheck.Mismatch), firstDifferences(report.CrossCheck.Mismatch))
	}
	if report.CrossCheck.Result == "unverifiable" && !assumeContinuous {
		d.wal.Close()
		return report, fmt.Errorf("legacy material cannot be cross-checked (%s); re-run with -assume-continuous to adopt it as a declared assumption", firstDifferences(report.CrossCheck.Mismatch))
	}
	if report.CrossCheck.Result == "unverifiable" {
		report.CrossCheck.Assumption = true
	}
	if report.RecoveredTail != 0 && d.wal.Adoption == nil {
		// The legacy tail is exactly what a normal restart would drop. The
		// upgrade may do the same, but only with an explicit request, and it
		// reports how many bytes were dropped.
		if !cutTail {
			d.wal.Close()
			return report, fmt.Errorf("legacy journal ends in an incomplete frame of %d bytes; re-run with -cut-tail to discard it (the complete prefix stays readable)", report.RecoveredTail)
		}
		if err = d.wal.TruncateTail(); err != nil {
			d.wal.Close()
			return report, err
		}
		report.CutTailBytes = report.RecoveredTail
	}
	if d.wal.Base > d.sequence {
		d.Close()
		return report, fmt.Errorf("legacy journal base %d is newer than checkpoint %d; missing history cannot be reconstructed", d.wal.Base, d.sequence)
	}
	if err = d.wal.Close(); err != nil {
		return report, err
	}

	journalBytes := fileSize(journalPath)
	digest, err := journal.DigestPrefix(journalPath, journalBytes)
	if err != nil {
		return report, err
	}
	checkpointDigest, err := journal.DigestFile(checkpointPath)
	if err != nil {
		return report, err
	}
	fingerprint, err := identity.New()
	if err != nil {
		return report, err
	}
	lineage := identity.Lineage(identity.LegacyFormat, identity.CurrentFormat, digest[:32])
	prov := identity.Root(fingerprint, "", lineage)

	adoptionRecord := journal.Adoption{
		Format:           identity.CurrentFormat,
		Provenance:       prov,
		LegacyBase:       d.wal.Base,
		LegacyBytes:      journalBytes,
		LegacyRecords:    len(d.wal.Records),
		LegacyDigest:     digest,
		LegacyCheckpoint: true,
		CheckpointBackup: filepath.Base(legacyCheckpoint),
		CheckpointDigest: checkpointDigest,
		AdoptedAt:        now.UTC().Format(time.RFC3339Nano),
	}

	// Step 4: move the legacy checkpoint aside. Rename keeps the bytes intact
	// for rollback while making the directory non-servable as v1-by-default.
	if _, err = os.Stat(legacyCheckpoint); err == nil {
		// A previous interrupted upgrade already moved it; require the
		// currently named checkpoint to be absent.
		if _, statErr := os.Stat(checkpointPath); statErr != nil {
			return report, fmt.Errorf("interrupted upgrade found: legacy checkpoint already at %s; re-run to complete", filepath.Base(legacyCheckpoint))
		}
		return report, fmt.Errorf("refusing to overwrite existing %s", filepath.Base(legacyCheckpoint))
	}
	if err = os.Rename(checkpointPath, legacyCheckpoint); err != nil {
		return report, fmt.Errorf("stage legacy checkpoint: %w", err)
	}
	if err = syncDir(absolute); err != nil {
		return report, err
	}
	report.LegacyCheckpoint = filepath.Base(legacyCheckpoint)

	// Step 5: install the identity-bearing checkpoint at the same sequence.
	image.Format = identity.CurrentFormat
	image.Provenance = &prov
	if err = snapshot.Write(checkpointPath, image); err != nil {
		_ = os.Rename(legacyCheckpoint, checkpointPath)
		return report, fmt.Errorf("write upgraded checkpoint: %w", err)
	}

	// Step 6: install the adoption record. This is the commit point.
	if err = journal.WriteAdoption(absolute, adoptionRecord); err != nil {
		// The checkpoint now claims an identity the journal cannot confirm.
		// Revert to the untouched legacy image so the directory is still
		// readable by the previous version.
		_ = os.Remove(checkpointPath)
		_ = os.Rename(legacyCheckpoint, checkpointPath)
		_ = syncDir(absolute)
		return report, fmt.Errorf("write adoption record: %w", err)
	}

	return finishUpgrade(absolute, lock, adoptionRecord, report)
}

// finishUpgrade verifies the converted directory through the serving path and
// fills in the report from the identity that was actually installed.
func finishUpgrade(dir string, lock *os.File, adoption journal.Adoption, report UpgradeReport) (UpgradeReport, error) {
	opened, err := openDir(dir, openOptions{locked: lock})
	if err != nil {
		return report, fmt.Errorf("verify upgraded directory: %w", err)
	}
	sequence := opened.sequence
	if err = opened.Close(); err != nil {
		return report, err
	}
	prov := adoption.Provenance
	report.Format = identity.CurrentFormat
	report.ReadableFormats = identity.CompatibleRange()
	report.Instance = prov
	report.LegacyJournal = adoption
	report.RetainedPrefix = adoption.LegacyBytes
	report.Records = adoption.LegacyRecords
	report.CheckpointSeq = sequence
	report.Head = sequence
	report.Rollback = RollbackPlan{
		Executable:   true,
		Command:      fmt.Sprintf("laminad rollback -data %s", dir),
		Checkpoint:   filepath.Join(dir, adoption.CheckpointBackup),
		JournalBytes: adoption.LegacyBytes,
		Note:         "keeps the v1 prefix in place; drops identity-bearing frames appended after the conversion",
	}
	return report, nil
}

// classifyJournal reads only the magic of an on-disk journal.
func classifyJournal(path string) (format int, legacy bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, false, fmt.Errorf("journal: %w", err)
	}
	defer f.Close()
	var magic [4]byte
	if _, err = io.ReadFull(f, magic[:]); err != nil {
		return 0, false, fmt.Errorf("journal header: %w", err)
	}
	switch string(magic[:]) {
	case "LMW2":
		return identity.CurrentFormat, false, nil
	case "LMW1":
		return identity.LegacyFormat, true, nil
	default:
		return 0, false, fmt.Errorf("invalid journal magic %q", string(magic[:]))
	}
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// RollbackReport describes the restoration of a legacy directory after an
// interrupted or completed in-place upgrade.
type RollbackReport struct {
	Status     string `json:"status"`
	Instance   string `json:"restored_instance,omitempty"`
	Checkpoint string `json:"checkpoint"`
	Dropped    int64  `json:"dropped_identity_frames_bytes"`
	Notice     string `json:"notice"`
}

// Rollback is the explicit inverse of Upgrade. It requires the adoption
// record written at the upgrade commit point, verifies that the retained v1
// prefix still matches it, restores the preserved v1 checkpoint and cuts the
// journal back to the v1 prefix. It then removes the identity artifacts.
//
// The operation is idempotent: running it on a directory that is already
// legacy is a no-op. It refuses to run when the prefix has changed, because
// that means the data is no longer the material that was adopted.
func Rollback(dir string) (RollbackReport, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return RollbackReport{}, err
	}
	lock, err := journal.LockDir(absolute)
	if err != nil {
		return RollbackReport{}, err
	}
	defer lock.Close()
	checkpointPath := filepath.Join(absolute, CheckpointName)
	journalPath := filepath.Join(absolute, journal.JournalFile)
	adoptionPath := filepath.Join(absolute, journal.IdentityFile)

	adoption, err := journal.ReadAdoption(absolute)
	if err != nil {
		return RollbackReport{}, err
	}
	if adoption == nil {
		_, checkpointLegacy, _ := snapshot.ReadCompat(checkpointPath)
		_, journalLegacy, classifyErr := classifyJournal(journalPath)
		if classifyErr != nil {
			return RollbackReport{}, classifyErr
		}
		if checkpointLegacy && journalLegacy {
			return RollbackReport{Status: "already-legacy", Checkpoint: checkpointPath, Notice: "directory is already in legacy format"}, nil
		}
		return RollbackReport{}, fmt.Errorf("no adoption record and not a legacy directory; refusing to guess")
	}
	if err = adoption.Validate(); err != nil {
		return RollbackReport{}, err
	}
	if !adoption.LegacyCheckpoint {
		return RollbackReport{}, fmt.Errorf("the upgrade did not preserve the legacy checkpoint; rollback is not executable for this directory")
	}
	preserved := filepath.Join(absolute, adoption.CheckpointBackup)
	if filepath.Base(adoption.CheckpointBackup) != adoption.CheckpointBackup {
		return RollbackReport{}, fmt.Errorf("adoption record names an invalid checkpoint")
	}
	digest, err := journal.DigestFile(preserved)
	if err != nil {
		return RollbackReport{}, fmt.Errorf("preserved legacy checkpoint: %w", err)
	}
	if digest != adoption.CheckpointDigest {
		return RollbackReport{}, fmt.Errorf("preserved legacy checkpoint does not match the adoption record")
	}
	prefix, err := journal.DigestPrefix(journalPath, adoption.LegacyBytes)
	if err != nil {
		return RollbackReport{}, err
	}
	if prefix != adoption.LegacyDigest {
		return RollbackReport{}, fmt.Errorf("retained legacy prefix no longer matches the adoption record; refusing to roll back onto different bytes")
	}
	info, err := os.Stat(journalPath)
	if err != nil {
		return RollbackReport{}, err
	}
	if info.Size() < adoption.LegacyBytes {
		return RollbackReport{}, fmt.Errorf("journal is shorter than the adopted prefix")
	}
	// Restore the legacy checkpoint first: until it lands, the directory is
	// still non-serving under the v2 identity, which is the safe side.
	if _, err = os.Stat(checkpointPath); err == nil {
		_ = os.Remove(checkpointPath)
	}
	if err = os.Rename(preserved, checkpointPath); err != nil {
		return RollbackReport{}, fmt.Errorf("restore legacy checkpoint: %w", err)
	}
	if err = syncDir(absolute); err != nil {
		return RollbackReport{}, err
	}
	if err = truncateFile(journalPath, adoption.LegacyBytes); err != nil {
		return RollbackReport{}, fmt.Errorf("cut identity-bearing suffix: %w", err)
	}
	if err = os.Remove(adoptionPath); err != nil && !os.IsNotExist(err) {
		return RollbackReport{}, err
	}
	if err = syncDir(absolute); err != nil {
		return RollbackReport{}, err
	}
	report := RollbackReport{
		Status:     "rolled-back",
		Instance:   adoption.Provenance.Instance,
		Checkpoint: checkpointPath,
		Dropped:    info.Size() - adoption.LegacyBytes,
		Notice:     "the v1 prefix, legacy checkpoint and all legacy records are byte-identical to the adopted material; only identity-bearing records appended after the upgrade were removed",
	}
	// Verify the restored directory through the legacy admission path.
	check, err := openDir(absolute, openOptions{allowLegacy: true, locked: lock})
	if err != nil {
		return report, fmt.Errorf("verify rolled-back directory: %w", err)
	}
	keys, versions := check.store.Count()
	_ = keys
	_ = versions
	if err = check.Close(); err != nil {
		return report, err
	}
	return report, nil
}

func truncateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}

// resumeFromAdoption completes a conversion that already passed its commit
// point. It never invents a new identity and never rewrites legacy bytes.
func resumeFromAdoption(dir string, lock *os.File, adoption journal.Adoption, report UpgradeReport) (UpgradeReport, error) {
	if err := adoption.Validate(); err != nil {
		return report, err
	}
	checkpointPath := filepath.Join(dir, CheckpointName)
	legacyPath := filepath.Join(dir, "checkpoint.v1.lmi")
	_, checkpointLegacy, checkpointErr := snapshot.ReadCompat(checkpointPath)
	if checkpointErr != nil && !os.IsNotExist(checkpointErr) {
		return report, checkpointErr
	}
	if checkpointErr == nil && !checkpointLegacy {
		// The serving checkpoint is already in place; just verify.
		report.Status = "already-upgraded"
		return finishUpgrade(dir, lock, adoption, report)
	}
	var image snapshot.Image
	var legacy bool
	var readErr error
	if checkpointErr == nil {
		image, legacy, readErr = snapshot.ReadCompat(checkpointPath)
	} else {
		image, legacy, readErr = snapshot.ReadCompat(legacyPath)
	}
	if readErr != nil {
		return report, fmt.Errorf("resume: no usable checkpoint: %w", readErr)
	}
	if !legacy {
		return report, fmt.Errorf("resume: checkpoint is already identity-bearing but does not carry the adopted identity")
	}
	if adoption.LegacyCheckpoint {
		digest, err := journal.DigestFile(legacyPath)
		if err != nil {
			return report, fmt.Errorf("resume: preserved legacy checkpoint: %w", err)
		}
		if adoption.CheckpointDigest == "" || digest != adoption.CheckpointDigest {
			return report, fmt.Errorf("resume: preserved legacy checkpoint does not match the adoption record")
		}
	}
	prov := adoption.Provenance
	image.Format = identity.CurrentFormat
	image.Provenance = &prov
	if err := snapshot.Write(checkpointPath, image); err != nil {
		return report, err
	}
	report.Status = "upgraded-after-interruption"
	report.CrossCheck = CrossCheck{Method: "adoption-record", Result: "resumed", Mismatch: []string{"cross-check ran before the interrupted commit; the adopted prefix is bound by its digest"}}
	return finishUpgrade(dir, lock, adoption, report)
}

// CrossCheck names the evidence reconciliation performed on legacy material.
type CrossCheck struct {
	Method     string   `json:"method"`
	Result     string   `json:"result"`
	Assumption bool     `json:"assumption,omitempty"`
	Mismatch   []string `json:"mismatch,omitempty"`
}

// reconcile replays the retained legacy journal from an empty database and
// compares the state at the checkpoint sequence with the checkpoint image.
// A legacy journal that starts at 0 contains the full history, so the two
// artifacts must agree on every visible row, version chain, catalog entry and
// retention horizon. Disagreement is the signature of a checkpoint and a
// journal produced by different sources that merely happen to share sequence
// numbers; it is reported rather than repaired.
func reconcile(d *DB, image snapshot.Image) CrossCheck {
	if d.wal.Base != 0 {
		return CrossCheck{Method: "journal-replay-vs-checkpoint", Result: "unverifiable", Mismatch: []string{fmt.Sprintf("journal starts at sequence %d, so the history before it is absent", d.wal.Base)}}
	}
	// Records are handed to consumers in the interpretable current layout.
	rebuild := &DB{catalog: catalog.New(), store: versions.New(), transactions: map[string]*Transaction{}, format: identity.CurrentFormat}
	for _, r := range d.wal.Records {
		if r.Sequence > image.Sequence {
			break
		}
		if err := rebuild.apply(r); err != nil {
			return CrossCheck{Method: "journal-replay-vs-checkpoint", Result: "unverifiable", Mismatch: []string{err.Error()}}
		}
	}
	out := CrossCheck{Method: "journal-replay-vs-checkpoint", Result: "verified"}
	if image.Provenance != nil {
		// The image is validated separately; this comparison is about state.
	}
	if image.Horizon != rebuild.horizon {
		out.Mismatch = append(out.Mismatch, fmt.Sprintf("horizon image=%d journal=%d", image.Horizon, rebuild.horizon))
	}
	for name, r := range image.Catalog.Relations {
		other, ok := rebuild.catalog.Relations[name]
		if !ok || other.Created != r.Created || other.Primary != r.Primary || fmt.Sprint(other.Columns) != fmt.Sprint(r.Columns) {
			out.Mismatch = append(out.Mismatch, "catalog relation "+name)
		}
	}
	for name := range rebuild.catalog.Relations {
		if _, ok := image.Catalog.Relations[name]; !ok {
			out.Mismatch = append(out.Mismatch, "journal-only relation "+name)
		}
	}
	for name := range image.Catalog.Indexes {
		if _, ok := rebuild.catalog.Indexes[name]; !ok {
			out.Mismatch = append(out.Mismatch, "catalog index "+name)
		}
	}
	for name := range rebuild.catalog.Indexes {
		if _, ok := image.Catalog.Indexes[name]; !ok {
			out.Mismatch = append(out.Mismatch, "journal-only index "+name)
		}
	}
	for name, rows := range image.Store.Tables {
		other := rebuild.store.Tables[name]
		for key, chain := range rows {
			otherChain, ok := other[key]
			if !ok || len(otherChain) != len(chain) {
				out.Mismatch = append(out.Mismatch, fmt.Sprintf("version chain %s/%s", name, key))
				continue
			}
			for i := range chain {
				if chain[i].Sequence != otherChain[i].Sequence || chain[i].Deleted != otherChain[i].Deleted || !sameRow(chain[i].Row, otherChain[i].Row) {
					out.Mismatch = append(out.Mismatch, fmt.Sprintf("version chain %s/%s", name, key))
					break
				}
			}
		}
		for key := range other {
			if _, ok := rows[key]; !ok {
				out.Mismatch = append(out.Mismatch, fmt.Sprintf("journal-only key %s/%s", name, key))
			}
		}
	}
	for name := range rebuild.store.Tables {
		if _, ok := image.Store.Tables[name]; !ok {
			out.Mismatch = append(out.Mismatch, "journal-only table "+name)
		}
	}
	if len(out.Mismatch) > 0 {
		out.Result = "mismatch"
	}
	return out
}

func firstDifferences(items []string) string {
	const limit = 4
	out := ""
	for i, item := range items {
		if i == limit {
			out += fmt.Sprintf(", ... (%d more)", len(items)-limit)
			break
		}
		if i > 0 {
			out += ", "
		}
		out += item
	}
	return out
}

// sameRow compares normalized rows by column, so map iteration order in the
// JSON representation cannot create false differences.
func sameRow(a, b datum.Row) bool {
	if len(a) != len(b) {
		return false
	}
	for column, value := range a {
		other, ok := b[column]
		if !ok || value.Key() != other.Key() {
			return false
		}
	}
	return true
}

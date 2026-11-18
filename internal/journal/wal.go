package journal

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"lamina/internal/identity"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	MaxFrame = 16 << 20

	JournalFile  = "journal.lmw"
	IdentityFile = "journal.lmw.id"

	magicV1 = "LMW1"
	magicV2 = "LMW2"

	headerV1Size = 16
	headerV2Size = 216
	// checksum at 184:216 covers 0:184.
	checksumOffset = 184
)

// Adoption is the durable record that a legacy (identity-less) journal was
// admitted in place. The digest binds the retained v1 prefix: swapping in a
// journal from another legacy directory, or losing part of the prefix, is
// detected before any record is replayed.
type Adoption struct {
	Format        int                 `json:"format"`
	Provenance    identity.Provenance `json:"provenance"`
	LegacyBase    uint64              `json:"legacy_base"`
	LegacyBytes   int64               `json:"legacy_bytes"`
	LegacyRecords int                 `json:"legacy_records"`
	LegacyDigest  string              `json:"legacy_sha256"`
	// LegacyCheckpoint is true when the v1 image was preserved in the same
	// directory for rollback; CheckpointBackup names it and CheckpointDigest
	// binds its bytes.
	LegacyCheckpoint bool   `json:"legacy_checkpoint"`
	CheckpointBackup string `json:"checkpoint_backup,omitempty"`
	CheckpointDigest string `json:"legacy_checkpoint_sha256,omitempty"`
	AdoptedAt        string `json:"adopted_at"`
}

func (a Adoption) Validate() error {
	if a.Format != identity.CurrentFormat {
		return fmt.Errorf("adoption format %d not supported", a.Format)
	}
	if err := a.Provenance.Check(); err != nil {
		return err
	}
	if a.LegacyBytes <= headerV1Size {
		return fmt.Errorf("adoption must cover journal header and at least the retained frames")
	}
	if len(a.LegacyDigest) != 64 {
		return fmt.Errorf("adoption requires a sha256 digest of the retained legacy prefix")
	}
	if a.LegacyRecords < 0 {
		return fmt.Errorf("invalid adoption record count")
	}
	return nil
}

// Options controls how an existing journal is admitted.
type Options struct {
	// Base is the checkpoint sequence the caller is about to use. The
	// journal's own base may not be newer than this.
	Base uint64
	// Expect is the identity the caller already read from the checkpoint.
	// Every identity-bearing journal must match it.
	Expect *identity.Provenance
	// Create supplies the identity when the journal file is created empty.
	// A nil value refuses to invent an identity.
	Create *identity.Provenance
	// AllowLegacy opens a v1 journal without identity. Only the upgrade and
	// rollback tools set this; the serving path always rejects it.
	AllowLegacy bool
	// Locked is an already-held directory lock. The caller keeps ownership
	// and the WAL closes it, matching the single-owner contract.
	Locked *os.File
	// NoTruncate leaves an incomplete final frame in place. The upgrade tool
	// validates the whole directory before deciding to discard a torn tail.
	NoTruncate bool
}

type WAL struct {
	Base          uint64
	ownLock       bool
	fresh         bool
	Provenance    identity.Provenance
	HasIdentity   bool
	Legacy        bool
	Adoption      *Adoption
	file          *os.File
	lock          *os.File
	Records       []Record
	RecoveredTail int64
	Poisoned      bool
	dir           string
	end           int64
	noTruncate    bool
}

func Open(dir string, opt Options) (*WAL, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock := opt.Locked
	ownLock := false
	if lock == nil {
		var err error
		lock, err = LockDir(dir)
		if err != nil {
			return nil, err
		}
		ownLock = true
	}
	path := filepath.Join(dir, JournalFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		lock.Close()
		return nil, err
	}
	w := &WAL{file: f, lock: lock, Base: opt.Base, dir: dir, noTruncate: opt.NoTruncate, ownLock: ownLock}
	info, err := f.Stat()
	if err != nil {
		w.Close()
		return nil, err
	}
	if info.Size() == 0 {
		if opt.Create == nil {
			w.Close()
			return nil, fmt.Errorf("journal is empty and no identity is available")
		}
		if err = writeHeaderV2(f, *opt.Create, opt.Base); err != nil {
			w.Close()
			return nil, err
		}
		if err = f.Sync(); err != nil {
			w.Close()
			return nil, err
		}
		w.Provenance = *opt.Create
		w.HasIdentity = true
		w.fresh = true
		if _, err = f.Seek(0, io.SeekEnd); err != nil {
			w.Close()
			return nil, err
		}
		return w, nil
	}
	if err = w.load(opt); err != nil {
		w.Close()
		return nil, err
	}
	if err = w.recover(); err != nil {
		w.Close()
		return nil, err
	}
	if err = syncDirectory(dir); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) load(opt Options) error {
	var magic [4]byte
	if _, err := io.ReadFull(w.file, magic[:]); err != nil {
		return fmt.Errorf("journal header: %w", err)
	}
	switch string(magic[:]) {
	case magicV2:
		if _, err := w.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		header, err := readHeaderV2(w.file)
		if err != nil {
			return err
		}
		path := filepath.Join(w.dir, IdentityFile)
		if _, statErr := os.Stat(path); statErr == nil {
			return fmt.Errorf("journal has both a v2 identity header and a legacy adoption record")
		} else if !os.IsNotExist(statErr) {
			return statErr
		}
		if header.Base > opt.Base {
			return fmt.Errorf("journal requires newer checkpoint")
		}
		if opt.Expect != nil {
			if err = identity.CheckSame(header.Provenance, *opt.Expect); err != nil {
				return err
			}
		}
		w.Base = header.Base
		w.Provenance = header.Provenance
		w.HasIdentity = true
		return nil
	case magicV1:
		return w.loadLegacy(opt)
	default:
		return fmt.Errorf("invalid journal magic %q", string(magic[:]))
	}
}

func (w *WAL) loadLegacy(opt Options) error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var header [headerV1Size]byte
	if _, err := io.ReadFull(w.file, header[:]); err != nil {
		return fmt.Errorf("journal header: %w", err)
	}
	if crc32.ChecksumIEEE(header[:12]) != binary.BigEndian.Uint32(header[12:]) {
		return fmt.Errorf("invalid journal header")
	}
	base := binary.BigEndian.Uint64(header[4:12])
	if base > opt.Base {
		return fmt.Errorf("journal requires newer checkpoint")
	}
	adoption, err := ReadAdoption(w.dir)
	if err != nil {
		return err
	}
	if adoption == nil {
		if !opt.AllowLegacy {
			return fmt.Errorf("legacy journal has no identity; run the upgrade before serving")
		}
		w.Base = base
		w.Legacy = true
		return nil
	}
	if err = adoption.Validate(); err != nil {
		return fmt.Errorf("journal adoption record: %w", err)
	}
	if adoption.LegacyBase != base {
		return fmt.Errorf("adoption base %d does not match journal header base %d", adoption.LegacyBase, base)
	}
	if opt.Expect != nil {
		if err = identity.CheckSame(adoption.Provenance, *opt.Expect); err != nil {
			return err
		}
	}
	digest, err := DigestPrefix(filepath.Join(w.dir, JournalFile), adoption.LegacyBytes)
	if err != nil {
		return fmt.Errorf("adopted journal prefix: %w", err)
	}
	if digest != adoption.LegacyDigest {
		return fmt.Errorf("adopted journal prefix digest mismatch")
	}
	w.Base = base
	w.Provenance = adoption.Provenance
	w.HasIdentity = true
	w.Adoption = adoption
	return nil
}

func (w *WAL) recover() error {
	size, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	headerLen := int64(headerV1Size)
	if w.HasIdentity && w.Adoption == nil && !w.Legacy {
		headerLen = headerV2Size
	}
	if _, err = w.file.Seek(headerLen, io.SeekStart); err != nil {
		return err
	}
	offset := headerLen
	last := w.Base
	legacyEnd := int64(-1)
	legacyCount := 0
	if w.Adoption != nil {
		legacyEnd = w.Adoption.LegacyBytes
		if size < legacyEnd {
			return fmt.Errorf("adopted journal is shorter than its adoption record")
		}
	}
	for {
		var head [8]byte
		n, err := io.ReadFull(w.file, head[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			w.RecoveredTail = int64(n)
			break
		}
		if err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(head[:4])
		if length == 0 || length > MaxFrame {
			return fmt.Errorf("invalid journal length at %d", offset)
		}
		payload := make([]byte, length)
		n, err = io.ReadFull(w.file, payload)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			w.RecoveredTail = int64(8 + n)
			break
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(head[4:]) {
			return fmt.Errorf("journal checksum mismatch at %d", offset)
		}
		inLegacy := w.Legacy
		if w.Adoption != nil {
			if offset >= legacyEnd {
				inLegacy = false
			} else {
				if offset+int64(8+length) > legacyEnd {
					return fmt.Errorf("journal frame crosses adoption boundary at %d", offset)
				}
				inLegacy = true
			}
		}
		allowed := identity.CurrentFormat
		if inLegacy {
			allowed = identity.LegacyFormat
		}
		var r Record
		if err = json.Unmarshal(payload, &r); err != nil {
			return fmt.Errorf("journal json at %d: %w", offset, err)
		}
		if r.Format != allowed || r.Sequence != last+1 {
			return fmt.Errorf("journal sequence or format mismatch at %d", offset)
		}
		if allowed == identity.LegacyFormat {
			// Frames keep their on-disk v1 layout (they are never rewritten);
			// consumers always see the layout they can interpret.
			r.Format = identity.CurrentFormat
		}
		w.Records = append(w.Records, r)
		last = r.Sequence
		if inLegacy {
			legacyCount++
		}
		offset += int64(8 + length)
	}
	if w.Adoption != nil {
		if offset < legacyEnd {
			return fmt.Errorf("adopted journal prefix is incomplete")
		}
		if legacyCount != w.Adoption.LegacyRecords {
			return fmt.Errorf("adopted journal has %d legacy records, adoption record states %d", legacyCount, w.Adoption.LegacyRecords)
		}
		if last < w.Base+uint64(w.Adoption.LegacyRecords) {
			return fmt.Errorf("adopted journal does not contain its declared records")
		}
	}
	w.end = offset
	if w.RecoveredTail > 0 && !w.noTruncate {
		if w.Adoption != nil && offset < legacyEnd {
			return fmt.Errorf("adopted journal prefix is truncated; restore it from the upgrade record")
		}
		if err := w.truncate(offset); err != nil {
			return err
		}
	}
	_, err = w.file.Seek(0, io.SeekEnd)
	return err
}

// Fresh reports whether this call created an empty journal. An existing
// identity-bearing journal with no checkpoint is half-converted material, not
// a new database.
func (w *WAL) Fresh() bool { return w.fresh }

// TruncateTail removes the incomplete final frame that was observed during
// recovery. It is only called after the caller has fully validated the
// directory and decided the tail is incomplete, never for a corrupt frame.
func (w *WAL) TruncateTail() error {
	if w.RecoveredTail == 0 {
		return nil
	}
	if err := w.truncate(w.end); err != nil {
		return err
	}
	w.RecoveredTail = 0
	return nil
}

func (w *WAL) truncate(offset int64) error {
	if err := w.file.Truncate(offset); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Append(r Record) error {
	if w.Poisoned {
		return fmt.Errorf("journal is unavailable; restart required")
	}
	if r.Sequence != w.Base+uint64(len(w.Records))+1 {
		return fmt.Errorf("noncontiguous journal sequence")
	}
	r.Format = identity.CurrentFormat
	if w.Legacy {
		r.Format = identity.LegacyFormat
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(payload) > MaxFrame {
		return fmt.Errorf("commit exceeds journal frame limit")
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	if _, err = w.file.Write(frame); err != nil {
		w.Poisoned = true
		return err
	}
	if err = w.file.Sync(); err != nil {
		w.Poisoned = true
		return err
	}
	w.Records = append(w.Records, r)
	return nil
}

// Close releases the journal file and, when the WAL took the directory lock
// itself, the lock. An externally supplied lock stays with its owner.
func (w *WAL) Close() error {
	err := w.file.Close()
	if w.ownLock && w.lock != nil {
		w.lock.Close()
	}
	return err
}

// LockDir takes the exclusive single-process directory lock.
func LockDir(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lamina.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data directory already open: %w", err)
	}
	return lock, nil
}

func writeHeaderV2(f *os.File, p identity.Provenance, base uint64) error {
	if err := p.Check(); err != nil {
		return fmt.Errorf("journal identity: %w", err)
	}
	header := make([]byte, headerV2Size)
	copy(header[:4], magicV2)
	binary.BigEndian.PutUint64(header[4:12], base)
	binary.BigEndian.PutUint32(header[12:16], 0)
	writeID(header[16:48], p.Instance)
	writeID(header[48:80], p.Origin)
	writeID(header[80:112], p.Parent)
	if len(p.Lineage) == 0 || len(p.Lineage) > 64 {
		return fmt.Errorf("journal identity: lineage must be 1..64 characters")
	}
	header[112] = byte(len(p.Lineage))
	copy(header[113:177], p.Lineage)
	sum := sha256.Sum256(header[:checksumOffset])
	copy(header[checksumOffset:], sum[:])
	if _, err := f.Write(header); err != nil {
		return err
	}
	return nil
}

func readHeaderV2(f *os.File) (struct {
	Base       uint64
	Provenance identity.Provenance
}, error) {
	var out struct {
		Base       uint64
		Provenance identity.Provenance
	}
	header := make([]byte, headerV2Size)
	if _, err := io.ReadFull(f, header); err != nil {
		return out, fmt.Errorf("journal header: %w", err)
	}
	sum := sha256.Sum256(header[:checksumOffset])
	if subtle.ConstantTimeCompare(sum[:], header[checksumOffset:]) != 1 {
		return out, fmt.Errorf("journal header checksum mismatch")
	}
	if flags := binary.BigEndian.Uint32(header[12:16]); flags != 0 {
		return out, fmt.Errorf("journal header flags %d not supported", flags)
	}
	out.Base = binary.BigEndian.Uint64(header[4:12])
	out.Provenance = identity.Provenance{
		Instance: readID(header[16:48]),
		Origin:   readID(header[48:80]),
		Parent:   readID(header[80:112]),
		Lineage:  string(header[113 : 113+int(header[112])]),
	}
	if int(header[112]) == 0 || int(header[112]) > 64 {
		return out, fmt.Errorf("journal header lineage length invalid")
	}
	if err := out.Provenance.Check(); err != nil {
		return out, fmt.Errorf("journal header identity: %w", err)
	}
	return out, nil
}

func writeID(dst []byte, id string) {
	if id == "" {
		return
	}
	copy(dst, id)
}

func readID(src []byte) string {
	s := strings.TrimRight(string(src), "\x00")
	if s == "" {
		return ""
	}
	return s
}

// ReadAdoption loads the legacy adoption record if it exists.
func ReadAdoption(dir string) (*Adoption, error) {
	b, err := os.ReadFile(filepath.Join(dir, IdentityFile))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var a Adoption
	if err = json.Unmarshal(b, &a); err != nil {
		return nil, fmt.Errorf("journal adoption record: %w", err)
	}
	return &a, nil
}

// WriteAdoption atomically installs the adoption record.
func WriteAdoption(dir string, a Adoption) error {
	if err := a.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return AtomicFile(filepath.Join(dir, IdentityFile), b)
}

// DigestPrefix hashes the first size bytes of a file.
func DigestPrefix(path string, size int64) (string, error) {
	if size < 0 {
		return "", fmt.Errorf("negative prefix size")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.CopyN(h, f, size); err != nil {
		if err == io.EOF {
			return "", fmt.Errorf("file is shorter than %d bytes", size)
		}
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// DigestFile hashes a whole file.
func DigestFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// AtomicFile writes data to a temporary file, syncs it, renames it over the
// destination and syncs the directory.
func AtomicFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".lamina-id-")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

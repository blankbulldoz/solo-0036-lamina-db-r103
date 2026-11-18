package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"lamina/internal/catalog"
	"lamina/internal/identity"
	"lamina/internal/versions"
	"os"
)

const MaxImage = 128 << 20

// Image is the frozen state written to a checkpoint or exported as a backup.
// Format 2 images carry the instance identity they belong to; format 1 images
// are the identity-less legacy layout and are only accepted by explicit
// migration/restore paths.
type Image struct {
	Format     int                  `json:"format"`
	Sequence   uint64               `json:"sequence"`
	Horizon    uint64               `json:"horizon"`
	Provenance *identity.Provenance `json:"provenance,omitempty"`
	Catalog    *catalog.Catalog     `json:"catalog"`
	Store      *versions.Store      `json:"store"`
}

type Envelope struct {
	Format  int             `json:"format"`
	Digest  string          `json:"sha256"`
	Payload json.RawMessage `json:"payload"`
}

func Encode(image Image) ([]byte, error) {
	if image.Format != identity.CurrentFormat {
		return nil, fmt.Errorf("refusing to encode image format %d", image.Format)
	}
	if image.Provenance == nil {
		return nil, fmt.Errorf("refusing to encode image without instance identity")
	}
	b, err := json.Marshal(image)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	return json.Marshal(Envelope{Format: identity.CurrentFormat, Digest: hex.EncodeToString(h[:]), Payload: b})
}

// Decode accepts only identity-bearing images. It is the serving-path reader.
func Decode(b []byte) (Image, error) {
	image, legacy, err := DecodeCompat(b)
	if err != nil {
		return image, err
	}
	if legacy {
		return image, fmt.Errorf("legacy image has no instance identity; run the upgrade or restore it with -restore")
	}
	return image, nil
}

// DecodeCompat accepts both the current image format and the identity-less
// legacy format. The legacy flag tells callers they must adopt the image
// explicitly rather than treat it as belonging to an existing instance.
func DecodeCompat(b []byte) (Image, bool, error) {
	var image Image
	var env Envelope
	if len(b) > MaxImage {
		return image, false, fmt.Errorf("image exceeds limit")
	}
	if err := json.Unmarshal(b, &env); err != nil {
		return image, false, err
	}
	h := sha256.Sum256(env.Payload)
	if env.Format != identity.LegacyFormat && env.Format != identity.CurrentFormat {
		return image, false, fmt.Errorf("image envelope format %d not supported", env.Format)
	}
	if hex.EncodeToString(h[:]) != env.Digest {
		return image, false, fmt.Errorf("image digest mismatch")
	}
	if err := json.Unmarshal(env.Payload, &image); err != nil {
		return image, false, err
	}
	if image.Format != env.Format {
		return image, false, fmt.Errorf("image format %d does not match envelope format %d", image.Format, env.Format)
	}
	if env.Format == identity.LegacyFormat {
		if image.Provenance != nil {
			return image, false, fmt.Errorf("legacy image must not carry instance identity")
		}
		if err := validateBody(image); err != nil {
			return image, false, err
		}
		return image, true, nil
	}
	if !identity.CompatibleFormat(image.Format) {
		return image, false, fmt.Errorf("image format %d outside readable range %s", image.Format, identity.CompatibleRange())
	}
	if image.Provenance == nil {
		return image, false, fmt.Errorf("image has no instance identity")
	}
	if err := image.Provenance.Check(); err != nil {
		return image, false, fmt.Errorf("image identity: %w", err)
	}
	if err := Validate(image); err != nil {
		return image, false, err
	}
	return image, false, nil
}

func Read(path string) (Image, error) {
	f, err := os.Open(path)
	if err != nil {
		return Image{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxImage+1))
	if err != nil {
		return Image{}, err
	}
	return Decode(b)
}

func ReadCompat(path string) (Image, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return Image{}, false, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, MaxImage+1))
	if err != nil {
		return Image{}, false, err
	}
	return DecodeCompat(b)
}

func Validate(image Image) error {
	if image.Format != identity.CurrentFormat {
		return fmt.Errorf("invalid image format %d", image.Format)
	}
	if image.Provenance == nil {
		return fmt.Errorf("image has no instance identity")
	}
	if err := image.Provenance.Check(); err != nil {
		return fmt.Errorf("image identity: %w", err)
	}
	return validateBody(image)
}

// validateBody checks the structural invariants shared by both formats.
func validateBody(image Image) error {
	if image.Horizon > image.Sequence || image.Catalog == nil || image.Store == nil || image.Catalog.Relations == nil || image.Catalog.Indexes == nil || image.Store.Tables == nil {
		return fmt.Errorf("invalid image header")
	}
	for name, r := range image.Catalog.Relations {
		if name != r.Name || r.Created == 0 || r.Created > image.Sequence {
			return fmt.Errorf("invalid catalog sequence")
		}
		if err := r.Validate(); err != nil {
			return err
		}
	}
	for name, definition := range image.Catalog.Indexes {
		relation, ok := image.Catalog.Relations[definition.Relation]
		if !ok || name != definition.Name || !catalog.ValidName(name) || definition.Created < relation.Created || definition.Created > image.Sequence {
			return fmt.Errorf("invalid index metadata")
		}
		if _, ok := relation.Column(definition.Column); !ok {
			return fmt.Errorf("invalid index column")
		}
	}
	for name, rows := range image.Store.Tables {
		r, ok := image.Catalog.Relations[name]
		if !ok {
			return fmt.Errorf("image references unknown relation")
		}
		for key, chain := range rows {
			var last uint64
			if len(chain) == 0 {
				return fmt.Errorf("empty version chain")
			}
			for _, v := range chain {
				if v.Sequence <= last || v.Sequence > image.Sequence || v.Sequence <= r.Created {
					return fmt.Errorf("invalid version order")
				}
				last = v.Sequence
				if !v.Deleted {
					row, err := r.Normalize(v.Row)
					if err != nil || r.Key(row) != key {
						return fmt.Errorf("invalid stored tuple")
					}
				}
			}
		}
	}
	for _, definition := range image.Catalog.Indexes {
		if !definition.Unique {
			continue
		}
		seen := map[string]bool{}
		for _, chain := range image.Store.Tables[definition.Relation] {
			version, exists := versions.Visible(chain, image.Sequence)
			if !exists {
				continue
			}
			value := version.Row[definition.Column]
			if value.Null() {
				continue
			}
			if seen[value.Key()] {
				return fmt.Errorf("image violates unique index %s", definition.Name)
			}
			seen[value.Key()] = true
		}
	}
	return nil
}

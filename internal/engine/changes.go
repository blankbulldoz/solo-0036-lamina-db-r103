package engine

import "lamina/internal/journal"

type ChangePage struct {
	Records     []journal.Record `json:"records"`
	Cursor      uint64           `json:"cursor"`
	Head        uint64           `json:"head"`
	HistoryBase uint64           `json:"history_base"`
	Instance    string           `json:"instance"`
	More        bool             `json:"more"`
}

func (d *DB) Changes(after uint64, limit int) (ChangePage, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := ChangePage{Records: []journal.Record{}, Cursor: after, Head: d.sequence, HistoryBase: d.wal.Base, Instance: d.provenance.Instance}
	if err := d.usable(); err != nil {
		return out, err
	}
	if after < d.wal.Base {
		return out, fault("snapshot", "change history begins at %d", d.wal.Base)
	}
	if after > d.sequence || limit < 1 || limit > 1000 {
		return out, fault("invalid", "invalid change cursor or limit")
	}
	for _, r := range d.wal.Records {
		if r.Sequence <= after {
			continue
		}
		if len(out.Records) >= limit {
			out.More = true
			break
		}
		out.Records = append(out.Records, r)
		out.Cursor = r.Sequence
	}
	return out, nil
}

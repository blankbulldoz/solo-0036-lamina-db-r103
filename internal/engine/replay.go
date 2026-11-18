package engine

import (
	"fmt"
	"lamina/internal/datum"
	"lamina/internal/journal"
)

func (d *DB) apply(r journal.Record) error {
	if r.Sequence != d.sequence+1 || r.Format != d.format {
		return fmt.Errorf("invalid record sequence")
	}
	switch r.Kind {
	case "relation":
		if r.Relation == nil || r.Relation.Created != r.Sequence {
			return fmt.Errorf("invalid relation record")
		}
		if err := r.Relation.Validate(); err != nil {
			return err
		}
		if _, ok := d.catalog.Relations[r.Relation.Name]; ok {
			return fmt.Errorf("duplicate relation")
		}
		d.catalog.Relations[r.Relation.Name] = *r.Relation
		d.store.Ensure(r.Relation.Name)
	case "index":
		if r.Index == nil || r.Index.Created != r.Sequence {
			return fmt.Errorf("invalid index record")
		}
		if err := d.catalog.CheckIndex(*r.Index); err != nil {
			return err
		}
		d.catalog.Indexes[r.Index.Name] = *r.Index
	case "commit":
		seen := map[string]bool{}
		for _, m := range r.Writes {
			relation, err := d.catalog.Relation(m.Relation)
			if err != nil {
				return err
			}
			token := m.Relation + "\x00" + m.Key
			if seen[token] {
				return fmt.Errorf("duplicate mutation")
			}
			seen[token] = true
			if !m.Deleted {
				row, err := relation.Normalize(m.Row)
				if err != nil {
					return err
				}
				if relation.Key(row) != m.Key {
					return fmt.Errorf("stored primary mismatch")
				}
			}
		}
		d.store.Apply(r.Sequence, r.Writes)
	case "vacuum":
		if r.Horizon < d.horizon || r.Horizon > d.sequence {
			return fmt.Errorf("invalid vacuum horizon")
		}
		d.store.Vacuum(r.Horizon)
		d.horizon = r.Horizon
	default:
		return fmt.Errorf("unknown record kind %s", r.Kind)
	}
	d.sequence = r.Sequence
	return nil
}
func overlay(rows map[string]datum.Row, writes map[string]map[string]staged, table string) {
	for key, m := range writes[table] {
		if m.Mutation.Deleted {
			delete(rows, key)
		} else {
			rows[key] = datum.Clone(m.Mutation.Row)
		}
	}
}

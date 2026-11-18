package transport

import (
	"lamina/internal/engine"
	"net/http"
	"strconv"
)

func (s *Server) checkpoint(w http.ResponseWriter, r *http.Request) {
	seq, err := s.db.Checkpoint()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]uint64{"sequence": seq})
}
func (s *Server) vacuum(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Horizon uint64 `json:"horizon"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Vacuum(in.Horizon)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	data, err := s.db.Backup()
	if err != nil {
		respondError(w, err)
		return
	}
	w.Header().Set("Content-Disposition", `attachment; filename="lamina-backup.lmi"`)
	w.WriteHeader(200)
	_, _ = w.Write(data)
}
func (s *Server) identity(w http.ResponseWriter, r *http.Request) {
	write(w, 200, s.db.Identity())
}
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.Stats()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) changes(w http.ResponseWriter, r *http.Request) {
	after := uint64(0)
	limit := 100
	var err error
	if value := r.URL.Query().Get("after"); value != "" {
		after, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			respondError(w, &engine.Error{Code: "invalid", Message: "invalid after"})
			return
		}
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil {
			respondError(w, &engine.Error{Code: "invalid", Message: "invalid limit"})
			return
		}
	}
	out, err := s.db.Changes(after, limit)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}

package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BenMeehan/paytm-wallet/internal/logs"
	"github.com/BenMeehan/paytm-wallet/internal/metrics"
	"github.com/BenMeehan/paytm-wallet/internal/store"
)

type ctxKey int

const (
	corrKey ctxKey = iota
	userKey
)

type Server struct {
	store *store.Store
	log   *logs.Logger
}

func New(st *store.Store, lg *logs.Logger) *Server { return &Server{store: st, log: lg} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /metrics", metrics.Handler)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.Handle("POST /wallets", s.auth(s.wrap(s.handleCreateWallet, "POST /wallets")))
	mux.Handle("GET /wallets/{id}", s.auth(s.wrap(s.handleGetWallet, "GET /wallets/{id}")))
	mux.Handle("POST /transfers", s.auth(s.wrap(s.handleCreateTransfer, "POST /transfers")))
	mux.Handle("GET /transfers/{id}", s.auth(s.wrap(s.handleGetTransfer, "GET /transfers/{id}")))
	return s.recover(s.correlate(mux))
}

func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error(corrID(r), "panic", map[string]any{"panic": rec})
				writeErr(w, http.StatusInternalServerError, "internal_error", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid := r.Header.Get("X-Correlation-Id")
		if cid == "" {
			cid = uuid.NewString()
		}
		w.Header().Set("X-Correlation-Id", cid)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), corrKey, cid)))
	})
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, prefix) || strings.TrimSpace(h[len(prefix):]) == "" {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "missing bearer token")
			return
		}
		uid := strings.TrimSpace(h[len(prefix):])
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, uid)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) { r.code = code; r.ResponseWriter.WriteHeader(code) }

func (s *Server) wrap(next func(http.ResponseWriter, *http.Request), route string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next(rec, r)
		durMS := float64(time.Since(start).Microseconds()) / 1000.0
		metrics.IncHTTP(r.Method, route, rec.code)
		metrics.ObserveLatency(time.Since(start).Seconds())
		s.log.Request(corrID(r), r.Method, route, rec.code, durMS)
	})
}

func corrID(r *http.Request) string {
	if v, ok := r.Context().Value(corrKey).(string); ok {
		return v
	}
	return ""
}

func userID(r *http.Request) string {
	if v, ok := r.Context().Value(userKey).(string); ok {
		return v
	}
	return ""
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]string{"error": errCode, "message": msg})
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "unreadable body")
		return false
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return true
	}
	if err := json.Unmarshal(body, dst); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "invalid JSON body")
		return false
	}
	return true
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(metrics.StatsHTML()))
}

type createWalletRequest struct {
	InitialBalancePaise *int64 `json:"initial_balance_paise"`
}

func (s *Server) handleCreateWallet(w http.ResponseWriter, r *http.Request) {
	var req createWalletRequest
	if !decodeBody(w, r, &req) {
		return
	}
	var initial int64
	if req.InitialBalancePaise != nil {
		if *req.InitialBalancePaise < 0 {
			writeErr(w, http.StatusBadRequest, "bad_request", "initial_balance_paise must be >= 0")
			return
		}
		initial = *req.InitialBalancePaise
	}
	uid := userID(r)
	wallet, created, err := s.store.GetOrCreateWallet(r.Context(), uid, initial)
	if err != nil {
		s.log.Error(corrID(r), "wallet get-or-create failed", map[string]any{"err": err.Error()})
		writeErr(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	if created {
		metrics.WalletsCreated.Add(1)
		s.log.Event(corrID(r), "wallet_created", "wallet created",
			map[string]any{"wallet_id": wallet.ID, "user_id": wallet.UserID, "balance_paise": wallet.Balance})
		writeJSON(w, http.StatusCreated, wallet)
		return
	}
	s.log.Event(corrID(r), "wallet_found", "wallet already existed",
		map[string]any{"wallet_id": wallet.ID, "user_id": wallet.UserID, "balance_paise": wallet.Balance})
	writeJSON(w, http.StatusOK, wallet)
}

func (s *Server) handleGetWallet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !isUUID(id) {
		writeErr(w, http.StatusBadRequest, "bad_request", "invalid wallet id")
		return
	}
	wallet, err := s.store.GetWallet(r.Context(), id)
	if err != nil {
		s.writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, wallet)
}

type createTransferRequest struct {
	From           string `json:"from"`
	To             string `json:"to"`
	AmountPaise    int64  `json:"amount_paise"`
	IdempotencyKey string `json:"idempotency_key"`
}

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	var req createTransferRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.IdempotencyKey == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "idempotency_key is required")
		return
	}
	if req.AmountPaise <= 0 {
		writeErr(w, http.StatusBadRequest, "bad_request", "amount_paise must be a positive integer")
		return
	}
	if !isUUID(req.From) || !isUUID(req.To) {
		writeErr(w, http.StatusBadRequest, "bad_request", "from/to must be valid wallet ids")
		return
	}
	if req.From == req.To {
		writeErr(w, http.StatusBadRequest, "bad_request", "from and to must differ")
		return
	}

	transfer, outcome, err := s.store.ExecuteTransfer(r.Context(), store.TransferRequest{
		Key:         req.IdempotencyKey,
		From:        req.From,
		To:          req.To,
		AmountPaise: req.AmountPaise,
	})
	if err != nil {
		if errors.Is(err, store.ErrKeyConflict) {
			writeErr(w, http.StatusConflict, "idempotency_key_conflict",
				"idempotency key reused with a different body")
			return
		}
		s.writeStoreErr(w, r, err)
		return
	}

	cid := corrID(r)
	switch outcome {
	case store.OutcomeCompleted:
		metrics.TransfersCreated.Add(1)
		s.log.Event(cid, "transfer_completed", "transfer debited and credited",
			map[string]any{"transfer_id": transfer.ID, "from": transfer.From, "to": transfer.To,
				"amount_paise": transfer.AmountPaise, "status": transfer.Status})
		writeJSON(w, http.StatusCreated, transfer)
	case store.OutcomeDeclinedInsufficientFunds:
		metrics.TransfersDeclined.Add(1)
		s.log.Event(cid, "transfer_declined", "transfer declined: insufficient funds",
			map[string]any{"transfer_id": transfer.ID, "from": transfer.From, "to": transfer.To,
				"amount_paise": transfer.AmountPaise, "reason": "insufficient_funds"})
		writeJSON(w, http.StatusUnprocessableEntity, transfer)
	case store.OutcomeReplay:
		metrics.IdempotentReplays.Add(1)
		s.log.Event(cid, "idempotent_replay", "idempotency key replay, original result returned",
			map[string]any{"transfer_id": transfer.ID, "idempotency_key": transfer.Key, "status": transfer.Status})
		if transfer.Status == store.StatusCompleted {
			writeJSON(w, http.StatusCreated, transfer)
		} else {
			writeJSON(w, http.StatusUnprocessableEntity, transfer)
		}
	}
}

func (s *Server) handleGetTransfer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !isUUID(id) {
		writeErr(w, http.StatusBadRequest, "bad_request", "invalid transfer id")
		return
	}
	transfer, err := s.store.GetTransfer(r.Context(), id)
	if err != nil {
		s.writeStoreErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transfer)
}

func (s *Server) writeStoreErr(w http.ResponseWriter, r *http.Request, err error) {
	var wnf *store.WalletNotFoundError
	var tnf *store.TransferNotFoundError
	switch {
	case errors.As(err, &wnf):
		writeErr(w, http.StatusNotFound, "wallet_not_found", wnf.Error())
	case errors.As(err, &tnf):
		writeErr(w, http.StatusNotFound, "transfer_not_found", tnf.Error())
	default:
		s.log.Error(corrID(r), "store error", map[string]any{"err": err.Error()})
		writeErr(w, http.StatusInternalServerError, "internal_error", "internal error")
	}
}

func isUUID(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

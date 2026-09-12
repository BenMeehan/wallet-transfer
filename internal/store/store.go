package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS wallets (
	id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	user_id    text NOT NULL UNIQUE,
	balance    bigint NOT NULL DEFAULT 0 CHECK (balance >= 0),
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transfers (
	id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	idempotency_key text NOT NULL UNIQUE,
	from_wallet     uuid NOT NULL REFERENCES wallets(id),
	to_wallet       uuid NOT NULL REFERENCES wallets(id),
	amount_paise    bigint NOT NULL CHECK (amount_paise > 0),
	status          text NOT NULL,
	created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_wallet);
CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_wallet);
`

const (
	StatusCompleted                 = "completed"
	StatusDeclinedInsufficientFunds = "declined_insufficient_funds"
)

var ErrKeyConflict = errors.New("idempotency key reused with a different body")

type WalletNotFoundError struct{ ID string }

func (e *WalletNotFoundError) Error() string { return "wallet not found: " + e.ID }

type TransferNotFoundError struct{ ID string }

func (e *TransferNotFoundError) Error() string { return "transfer not found: " + e.ID }

type Wallet struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Balance   int64     `json:"balance"`
	CreatedAt time.Time `json:"created_at"`
}

type Transfer struct {
	ID          string    `json:"id"`
	Key         string    `json:"idempotency_key"`
	From        string    `json:"from"`
	To          string    `json:"to"`
	AmountPaise int64     `json:"amount_paise"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

type TransferRequest struct {
	Key         string
	From        string
	To          string
	AmountPaise int64
}

type Outcome int

const (
	OutcomeCompleted Outcome = iota
	OutcomeDeclinedInsufficientFunds
	OutcomeReplay
)

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) GetOrCreateWallet(ctx context.Context, userID string, initialBalancePaise int64) (Wallet, bool, error) {
	w := Wallet{UserID: userID}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO wallets (user_id, balance) VALUES ($1, $2)
		 ON CONFLICT (user_id) DO NOTHING
		 RETURNING id::text, balance, created_at`,
		userID, initialBalancePaise,
	).Scan(&w.ID, &w.Balance, &w.CreatedAt)
	if err == nil {
		return w, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Wallet{}, false, fmt.Errorf("insert wallet: %w", err)
	}
	err = s.pool.QueryRow(ctx,
		`SELECT id::text, balance, created_at FROM wallets WHERE user_id = $1`, userID,
	).Scan(&w.ID, &w.Balance, &w.CreatedAt)
	if err != nil {
		return Wallet{}, false, fmt.Errorf("select wallet: %w", err)
	}
	return w, false, nil
}

func (s *Store) GetWallet(ctx context.Context, id string) (Wallet, error) {
	var w Wallet
	err := s.pool.QueryRow(ctx,
		`SELECT id::text, user_id, balance, created_at FROM wallets WHERE id = $1::uuid`, id,
	).Scan(&w.ID, &w.UserID, &w.Balance, &w.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Wallet{}, &WalletNotFoundError{ID: id}
	}
	if err != nil {
		return Wallet{}, fmt.Errorf("select wallet: %w", err)
	}
	return w, nil
}

func (s *Store) GetTransfer(ctx context.Context, id string) (Transfer, error) {
	var t Transfer
	err := s.pool.QueryRow(ctx,
		`SELECT id::text, idempotency_key, from_wallet::text, to_wallet::text,
		        amount_paise, status, created_at
		 FROM transfers WHERE id = $1::uuid`, id,
	).Scan(&t.ID, &t.Key, &t.From, &t.To, &t.AmountPaise, &t.Status, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transfer{}, &TransferNotFoundError{ID: id}
	}
	if err != nil {
		return Transfer{}, fmt.Errorf("select transfer: %w", err)
	}
	return t, nil
}

func (s *Store) ExecuteTransfer(ctx context.Context, req TransferRequest) (Transfer, Outcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Transfer{}, 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(context.Background())

	fromID, err := uuid.Parse(req.From)
	if err != nil {
		return Transfer{}, 0, fmt.Errorf("parse from id: %w", err)
	}
	toID, err := uuid.Parse(req.To)
	if err != nil {
		return Transfer{}, 0, fmt.Errorf("parse to id: %w", err)
	}
	lo, hi := req.From, req.To
	if bytes.Compare(fromID[:], toID[:]) > 0 {
		lo, hi = req.To, req.From
	}

	// lock lower id first so A->B and B->A don't deadlock.
	// take the row locks before the transfer insert: the insert's FKs grab
	// share locks on both wallets, and upgrading those after the fact deadlocks.
	for _, wid := range []string{lo, hi} {
		var one int
		err := tx.QueryRow(ctx,
			`SELECT 1 FROM wallets WHERE id = $1::uuid FOR NO KEY UPDATE`, wid).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return Transfer{}, 0, &WalletNotFoundError{ID: wid}
		}
		if err != nil {
			return Transfer{}, 0, fmt.Errorf("lock wallet %s: %w", wid, err)
		}
	}

	var id string
	err = tx.QueryRow(ctx,
		`INSERT INTO transfers (idempotency_key, from_wallet, to_wallet, amount_paise, status)
		 VALUES ($1, $2::uuid, $3::uuid, $4, 'pending')
		 ON CONFLICT (idempotency_key) DO NOTHING
		 RETURNING id::text`,
		req.Key, req.From, req.To, req.AmountPaise,
	).Scan(&id)

	if errors.Is(err, pgx.ErrNoRows) {
		var t Transfer
		err := tx.QueryRow(ctx,
			`SELECT id::text, idempotency_key, from_wallet::text, to_wallet::text,
			        amount_paise, status, created_at
			 FROM transfers WHERE idempotency_key = $1`, req.Key,
		).Scan(&t.ID, &t.Key, &t.From, &t.To, &t.AmountPaise, &t.Status, &t.CreatedAt)
		if err != nil {
			return Transfer{}, 0, fmt.Errorf("read existing transfer: %w", err)
		}
		if t.From != req.From || t.To != req.To || t.AmountPaise != req.AmountPaise {
			return Transfer{}, 0, ErrKeyConflict
		}
		return t, OutcomeReplay, nil
	}
	if err != nil {
		return Transfer{}, 0, fmt.Errorf("insert transfer: %w", err)
	}

	ct, err := tx.Exec(ctx,
		`UPDATE wallets SET balance = balance - $2 WHERE id = $1::uuid AND balance >= $2`,
		req.From, req.AmountPaise)
	if err != nil {
		return Transfer{}, 0, fmt.Errorf("debit: %w", err)
	}
	if ct.RowsAffected() == 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE transfers SET status = $2 WHERE id = $1::uuid`, id, StatusDeclinedInsufficientFunds); err != nil {
			return Transfer{}, 0, fmt.Errorf("mark declined: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Transfer{}, 0, fmt.Errorf("commit declined: %w", err)
		}
		return Transfer{
			ID:          id,
			Key:         req.Key,
			From:        req.From,
			To:          req.To,
			AmountPaise: req.AmountPaise,
			Status:      StatusDeclinedInsufficientFunds,
		}, OutcomeDeclinedInsufficientFunds, nil
	}

	if _, err := tx.Exec(ctx,
		`UPDATE wallets SET balance = balance + $2 WHERE id = $1::uuid`, req.To, req.AmountPaise); err != nil {
		return Transfer{}, 0, fmt.Errorf("credit: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE transfers SET status = $2 WHERE id = $1::uuid`, id, StatusCompleted); err != nil {
		return Transfer{}, 0, fmt.Errorf("mark completed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Transfer{}, 0, fmt.Errorf("commit: %w", err)
	}
	return Transfer{
		ID:          id,
		Key:         req.Key,
		From:        req.From,
		To:          req.To,
		AmountPaise: req.AmountPaise,
		Status:      StatusCompleted,
	}, OutcomeCompleted, nil
}

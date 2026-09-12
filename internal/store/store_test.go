package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/BenMeehan/paytm-wallet/internal/store"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func walletPair(t *testing.T, s *store.Store, fund int64) (string, string) {
	t.Helper()
	ctx := context.Background()
	a, _, err := s.GetOrCreateWallet(ctx, fmt.Sprintf("a-%d-%d", time.Now().UnixNano(), fund), fund)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := s.GetOrCreateWallet(ctx, fmt.Sprintf("b-%d-%d", time.Now().UnixNano(), fund), fund)
	if err != nil {
		t.Fatal(err)
	}
	return a.ID, b.ID
}

func TestGetOrCreateRace(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	user := fmt.Sprintf("race-%d", time.Now().UnixNano())
	const n = 50
	var wg sync.WaitGroup
	ids := make(chan string, n)
	created := make(chan bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, c, err := s.GetOrCreateWallet(ctx, user, 0)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- w.ID
			created <- c
		}()
	}
	wg.Wait()
	close(ids)
	close(created)
	distinct := map[string]bool{}
	for id := range ids {
		distinct[id] = true
	}
	if len(distinct) != 1 {
		t.Fatalf("expected exactly 1 wallet, got %d: %v", len(distinct), distinct)
	}
	creations := 0
	for c := range created {
		if c {
			creations++
		}
	}
	if creations != 1 {
		t.Fatalf("expected exactly 1 create, got %d", creations)
	}
}

func TestIdempotencyStorm(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	from, to := walletPair(t, s, 100000)
	key := fmt.Sprintf("storm-%d", time.Now().UnixNano())
	const k = 30
	var wg sync.WaitGroup
	transfers := make(chan store.Transfer, k)
	errorsCh := make(chan error, k)
	for i := 0; i < k; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr, _, err := s.ExecuteTransfer(ctx, store.TransferRequest{Key: key, From: from, To: to, AmountPaise: 100})
			if err != nil {
				errorsCh <- err
				return
			}
			transfers <- tr
		}()
	}
	wg.Wait()
	close(transfers)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("unexpected error: %v", err)
	}
	distinct := map[string]bool{}
	for tr := range transfers {
		distinct[tr.ID] = true
	}
	if len(distinct) != 1 {
		t.Fatalf("expected 1 transfer row, got %d", len(distinct))
	}
	fw, _ := s.GetWallet(ctx, from)
	tw, _ := s.GetWallet(ctx, to)
	if fw.Balance != 99900 || tw.Balance != 100100 {
		t.Fatalf("exactly-once violated: from=%d to=%d", fw.Balance, tw.Balance)
	}
}

func TestSameKeyDifferentBodyConflicts(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	from, to := walletPair(t, s, 100000)
	key := fmt.Sprintf("conflict-%d", time.Now().UnixNano())
	if _, _, err := s.ExecuteTransfer(ctx, store.TransferRequest{Key: key, From: from, To: to, AmountPaise: 100}); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.ExecuteTransfer(ctx, store.TransferRequest{Key: key, From: from, To: to, AmountPaise: 200})
	if !errors.Is(err, store.ErrKeyConflict) {
		t.Fatalf("expected ErrKeyConflict, got %v", err)
	}
	fw, _ := s.GetWallet(ctx, from)
	if fw.Balance != 99900 {
		t.Fatalf("second debit applied: from=%d", fw.Balance)
	}
}

func TestNoOverdraft(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	from, to := walletPair(t, s, 1000)
	tr, outcome, err := s.ExecuteTransfer(ctx, store.TransferRequest{
		Key: fmt.Sprintf("over-%d", time.Now().UnixNano()), From: from, To: to, AmountPaise: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome != store.OutcomeDeclinedInsufficientFunds || tr.Status != store.StatusDeclinedInsufficientFunds {
		t.Fatalf("expected decline, got outcome=%v status=%s", outcome, tr.Status)
	}
	fw, _ := s.GetWallet(ctx, from)
	if fw.Balance != 1000 {
		t.Fatalf("decline applied partially: from=%d", fw.Balance)
	}
	// replay of the declined transfer returns the original declined result
	tr2, outcome2, err := s.ExecuteTransfer(ctx, store.TransferRequest{
		Key: tr.Key, From: from, To: to, AmountPaise: 5000,
	})
	if err != nil || outcome2 != store.OutcomeReplay || tr2.Status != store.StatusDeclinedInsufficientFunds || tr2.ID != tr.ID {
		t.Fatalf("expected identical declined replay, got %+v outcome=%v err=%v", tr2, outcome2, err)
	}
}

func TestCrossTransferNoDeadlock(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	a, b := walletPair(t, s, 10_000_000)
	const rounds = 100
	var wg sync.WaitGroup
	errs := make(chan error, rounds*2)
	for i := 0; i < rounds; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.ExecuteTransfer(ctx, store.TransferRequest{
				Key: fmt.Sprintf("ab-%d-%d", time.Now().UnixNano(), i), From: a, To: b, AmountPaise: 100,
			})
			if err != nil {
				errs <- err
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.ExecuteTransfer(ctx, store.TransferRequest{
				Key: fmt.Sprintf("ba-%d-%d", time.Now().UnixNano(), i), From: b, To: a, AmountPaise: 100,
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("cross-transfer error (deadlock?): %v", err)
	}
	wa, _ := s.GetWallet(ctx, a)
	wb, _ := s.GetWallet(ctx, b)
	if wa.Balance+wb.Balance != 20_000_000 {
		t.Fatalf("conservation broken: %d + %d", wa.Balance, wb.Balance)
	}
	if wa.Balance < 0 || wb.Balance < 0 {
		t.Fatalf("negative balance: a=%d b=%d", wa.Balance, wb.Balance)
	}
}

func TestConservationUnderContention(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const wallets = 4
	const transfers = 300
	const seed = 1_000_000
	ids := make([]string, wallets)
	sum := int64(0)
	for i := range ids {
		w, _, err := s.GetOrCreateWallet(ctx, fmt.Sprintf("c-%d-%d", time.Now().UnixNano(), i), seed)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = w.ID
		sum += seed
	}
	var wg sync.WaitGroup
	errs := make(chan error, transfers+wallets)
	for i := 0; i < transfers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a := i % wallets
			b := (i + 1 + i%2) % wallets // include adjacent and opposite directions
			amt := int64(1 + (i*37)%5000)
			_, _, err := s.ExecuteTransfer(ctx, store.TransferRequest{
				Key: fmt.Sprintf("ct-%d-%d", time.Now().UnixNano(), i), From: ids[a], To: ids[b], AmountPaise: amt,
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	for i := range ids { // guaranteed overdrafts must decline, not corrupt
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.ExecuteTransfer(ctx, store.TransferRequest{
				Key: fmt.Sprintf("ov-%d-%d", time.Now().UnixNano(), i), From: ids[i], To: ids[(i+1)%wallets], AmountPaise: 1_000_000_000_000,
			})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("contention error: %v", err)
	}
	after := int64(0)
	for _, id := range ids {
		w, err := s.GetWallet(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if w.Balance < 0 {
			t.Fatalf("negative balance %d on %s", w.Balance, id)
		}
		after += w.Balance
	}
	if sum != after {
		t.Fatalf("conservation broken: before=%d after=%d", sum, after)
	}
}

func TestWalletNotFound(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	from, _ := walletPair(t, s, 1000)
	_, _, err := s.ExecuteTransfer(ctx, store.TransferRequest{
		Key: fmt.Sprintf("nf-%d", time.Now().UnixNano()), From: from, To: "00000000-0000-0000-0000-000000000000", AmountPaise: 10,
	})
	var wnf *store.WalletNotFoundError
	if !errors.As(err, &wnf) {
		t.Fatalf("expected WalletNotFoundError, got %v", err)
	}
}

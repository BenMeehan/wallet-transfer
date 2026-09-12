# Wallet Transfer Service

Small Go and Postgres service for wallet creation and peer-to-peer transfers.
All monetary values are integer paise.

## Run locally

```sh
docker compose up --build
```

The API is available at `http://localhost:8080`.

Run the concurrency checks in another terminal:

```sh
python3 scripts/burst.py
```

## Authentication

API endpoints require a bearer token:

```text
Authorization: Bearer ben
```

For this exercise the token is used as the user identifier. This keeps auth deliberately simple so the focus stays on transfer correctness.

## API

### Create or get a wallet

```sh
curl -X POST http://localhost:8080/wallets \
  -H 'Authorization: Bearer ben' \
  -H 'Content-Type: application/json' \
  -d '{"initial_balance_paise":100000}'
```

`initial_balance_paise` is only applied when the wallet is first created. It exists to seed wallets for local testing.

### Get a wallet

```sh
curl http://localhost:8080/wallets/<wallet-id> \
  -H 'Authorization: Bearer ben'
```

### Create a transfer

```sh
curl -X POST http://localhost:8080/transfers \
  -H 'Authorization: Bearer ben' \
  -H 'Content-Type: application/json' \
  -d '{"from":"<from-wallet-id>","to":"<to-wallet-id>","amount_paise":500,"idempotency_key":"payment-001"}'
```

Successful transfers return `201`. Insufficient funds return `422` and persist a declined transfer, so retries return the same outcome. Reusing a key with different transfer details returns `409`.

### Other endpoints

- `GET /transfers/{id}`
- `GET /health`
- `GET /metrics`
- `GET /stats`

## Testing

Start Postgres first with `docker compose up -d db`, then run:

```sh
make test
```

This runs the integration tests in `internal/store` against real Postgres (set `TEST_DATABASE_URL` if it is not localhost). The tests cover:

- 50 goroutines creating a wallet for the same user at once, asserting one wallet is created.
- 30 goroutines firing the same transfer with the same idempotency key, asserting one debit and one credit.
- Reusing an idempotency key with a different body, asserting a conflict.
- A debit that overdraws, asserting the transfer is declined and the balance is untouched.
- 100 rounds of simultaneous A to B and B to A transfers, asserting no deadlocks and money conservation.
- 300 mixed concurrent transfers plus forced overdrafts, asserting the total is conserved and no balance goes negative.

`scripts/burst.py` does the same three probes over HTTP and is useful against any running instance:

```sh
python3 scripts/burst.py                      # local
BASE_URL=https://<app>.onrender.com python3 scripts/burst.py   # deployed
```

CI (`.github/workflows/ci.yml`) runs `go vet`, the test suite with `-race`, and a Docker build on every push.

The integration tests use real Postgres and cover concurrent wallet creation, retry storms, cross-direction transfers, insufficient funds, and conservation under load.

## Deployment

Live at [https://wallet-transfer-ht6p.onrender.com](https://wallet-transfer-ht6p.onrender.com). It runs on Render's free tier, uses a Supabase free Postgres database, and ships structured logs to BetterStack Logtail. Prometheus metrics are at [/metrics](https://wallet-transfer-ht6p.onrender.com/metrics).

Run the probes against it:

```sh
BASE_URL=https://wallet-transfer-ht6p.onrender.com python3 scripts/burst.py
```

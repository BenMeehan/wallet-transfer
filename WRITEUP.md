# Wallet Transfer Write-up

## Data model

`wallets` stores one row per user. `user_id` is unique and `balance` is a `bigint` in paise. A database check prevents a stored balance below zero.

`transfers` stores the request and its outcome. `idempotency_key` is unique. Amount, source wallet, destination wallet, and status are saved with the transfer. A transfer is either `completed` or `declined_insufficient_funds`.

## Simplest-Correct Mechanism and Rejected Alternatives

Each transfer is handled in one Postgres transaction.

1. Lock the two wallet rows in UUID order, lower UUID first, using `FOR NO KEY UPDATE`.
2. Insert the transfer row using `ON CONFLICT (idempotency_key) DO NOTHING`.
3. Debit with `UPDATE wallets SET balance = balance - $amount WHERE id = $from AND balance >= $amount`.
4. If the debit changed no row, save the declined status and commit.
5. Otherwise credit the recipient, mark the transfer completed, and commit.

The sorted lock order matters for a concurrent A-to-B and B-to-A pair. Both requests try to lock the same first wallet, so one waits instead of each request holding one wallet and waiting for the other.

The transfer insert happens after the wallet locks. The foreign keys on that insert take share locks on the wallets. Locking the wallets first avoids an upgrade deadlock between two transfers in opposite directions.

I chose this over serializable isolation because serializable would require retrying aborted transactions under normal contention. An in-process mutex would not work once there is more than one service instance. Advisory locks add another locking scheme without simplifying the transaction.

## Idempotency

The unique constraint on `transfers.idempotency_key` is the source of truth. It is written in the same transaction as the wallet updates. A duplicate request either waits for the original transaction and reads the saved transfer, or finds the existing committed row. It cannot create another debit.

The saved `from`, `to`, and `amount_paise` are compared with a replayed request. A different request body using the same key returns `409 Conflict`. Declined transfers are saved too, which means a retry returns the original decline rather than attempting the debit again.

## Consistency vs Availability

I prefer consistency here. The service relies on one Postgres primary as the authority for balances and transfer state. A database outage means transfers are unavailable rather than accepting a request that cannot be checked safely. This free-tier setup is single-region and does not attempt multi-region availability.

## AI Disclosure

I used AI (Opencode) as a coding assistant while building the service. I chose the language, database, API contract, transaction boundary, and locking approach. I used AI help for scaffolding, test cases, and investigating a locking problem during development. I reviewed the final behavior with the concurrency tests and burst script.

## Cost

Render free web service, Supabase free Postgres, and BetterStack free logs keep the demo at ₹0.

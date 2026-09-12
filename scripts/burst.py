#!/usr/bin/env python3
"""Hit the wallet API with the three concurrency probes.

  BASE_URL=http://localhost:8080 python3 scripts/burst.py
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = os.environ.get("BASE_URL", "http://localhost:8080").rstrip("/")
N = int(os.environ.get("N", "50"))   # concurrent get-or-create
K = int(os.environ.get("K", "30"))   # same-key retry storm
M = int(os.environ.get("M", "100"))  # contention transfers


def req(method, path, token, body=None):
    data = None if body is None else json.dumps(body).encode()
    r = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw.decode(errors="replace")}


def create_wallet(token, balance=0):
    body = {} if balance == 0 else {"initial_balance_paise": balance}
    code, w = req("POST", "/wallets", token, body)
    if code not in (200, 201):
        raise RuntimeError("POST /wallets failed: %s %s" % (code, w))
    return w


def get_balance(token, wallet_id):
    code, w = req("GET", "/wallets/" + wallet_id, token)
    if code != 200:
        raise RuntimeError("GET /wallets failed: %s %s" % (code, w))
    return int(w["balance"])


def fail(msg):
    print("FAIL:", msg)
    sys.exit(1)


def probe_getorcreate():
    user = "u-%d-%d" % (time.time_ns(), os.getpid())
    ids = []
    with ThreadPoolExecutor(max_workers=N) as pool:
        futs = [pool.submit(req, "POST", "/wallets", user, {}) for _ in range(N)]
        for f in as_completed(futs):
            code, body = f.result()
            if code not in (200, 201) or "id" not in body:
                fail("get-or-create bad response %s %s" % (code, body))
            ids.append(body["id"])
    distinct = set(ids)
    print("get-or-create: %d concurrent POSTs for %s -> %d wallet(s)" % (N, user, len(distinct)))
    if len(distinct) != 1:
        fail("expected 1 wallet, got %d" % len(distinct))
    print("PASS: one wallet")


def probe_idempotent():
    tag = "s-%d" % time.time_ns()
    a = create_wallet(tag + "-a", 10_000_000)
    b = create_wallet(tag + "-b")
    body = {
        "from": a["id"],
        "to": b["id"],
        "amount_paise": 1000,
        "idempotency_key": tag + "-key",
    }
    results = []
    with ThreadPoolExecutor(max_workers=K) as pool:
        futs = [pool.submit(req, "POST", "/transfers", tag + "-a", body) for _ in range(K)]
        for f in as_completed(futs):
            results.append(f.result())

    transfer_ids = set()
    for code, t in results:
        if "id" not in t:
            fail("idempotent storm bad response %s %s" % (code, t))
        transfer_ids.add(t["id"])

    ba = get_balance(tag + "-a", a["id"])
    bb = get_balance(tag + "-b", b["id"])
    print("idempotent storm: %d same-key POSTs -> %d transfer id(s); A=%d B=%d" % (
        K, len(transfer_ids), ba, bb))
    if len(transfer_ids) != 1 or ba != 9_999_000 or bb != 1000:
        fail("expected one apply (A=9999000 B=1000)")
    print("PASS: one debit, one credit")


def probe_contention():
    tag = "c-%d" % time.time_ns()
    wallets = [create_wallet("%s-%d" % (tag, i), 10_000_000) for i in range(4)]
    ids = [w["id"] for w in wallets]
    before = sum(get_balance("%s-%d" % (tag, i), ids[i]) for i in range(4))

    jobs = []
    for i in range(M):
        a, b = i % 4, (i + 1) % 4
        amt = 1 + (i * 37) % 500
        if i % 2:
            a, b = b, a
        jobs.append((ids[a], ids[b], amt, "%s-k%d" % (tag, i)))
    for i in range(4):
        jobs.append((ids[i], ids[(i + 1) % 4], 1_000_000_000_000, "%s-over-%d" % (tag, i)))

    print("contention: %d concurrent transfers across 4 wallets (incl A->B and B->A)" % len(jobs))
    statuses = []
    with ThreadPoolExecutor(max_workers=50) as pool:
        futs = [
            pool.submit(
                req, "POST", "/transfers", tag,
                {"from": fr, "to": to, "amount_paise": amt, "idempotency_key": key},
            )
            for fr, to, amt, key in jobs
        ]
        for f in as_completed(futs):
            code, t = f.result()
            statuses.append(t.get("status", "http-%s" % code))

    from collections import Counter
    counts = Counter(statuses)
    print(" ", dict(counts))

    after = 0
    negatives = 0
    for i in range(4):
        bal = get_balance("%s-%d" % (tag, i), ids[i])
        after += bal
        if bal < 0:
            negatives += 1
    print("total before=%d after=%d" % (before, after))
    if before != after or negatives:
        fail("conservation broken or negative balance")
    print("PASS: conserved, no negatives")


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    probes = {
        "getorcreate": probe_getorcreate,
        "idempotent": probe_idempotent,
        "contention": probe_contention,
    }
    if which == "all":
        for name in ("getorcreate", "idempotent", "contention"):
            probes[name]()
        return
    if which not in probes:
        print("usage: burst.py [getorcreate|idempotent|contention|all]")
        sys.exit(2)
    probes[which]()


if __name__ == "__main__":
    main()

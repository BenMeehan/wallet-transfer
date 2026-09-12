package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	httpReqs sync.Map // "POST /wallets 201" -> *uint64

	latMu     sync.Mutex
	latCount  uint64
	latSum    float64
	latBucket [len(latBounds)]uint64

	TransfersCreated  atomic.Uint64
	TransfersDeclined atomic.Uint64
	IdempotentReplays atomic.Uint64
	WalletsCreated    atomic.Uint64

	startTime = time.Now()
)

var latBounds = [...]float64{0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

func IncHTTP(method, route string, code int) {
	key := method + " " + route + " " + strconv.Itoa(code)
	v, _ := httpReqs.LoadOrStore(key, new(uint64))
	atomic.AddUint64(v.(*uint64), 1)
}

func ObserveLatency(secs float64) {
	latMu.Lock()
	defer latMu.Unlock()
	latCount++
	latSum += secs
	for i, b := range latBounds {
		if secs <= b {
			latBucket[i]++
		}
	}
}

func Quantile(q float64) float64 {
	latMu.Lock()
	defer latMu.Unlock()
	if latCount == 0 {
		return 0
	}
	target := float64(latCount) * q
	prevBound, prevCum := 0.0, uint64(0)
	var cum uint64
	for i, b := range latBounds {
		cum += latBucket[i]
		if float64(cum) >= target {
			span := cum - prevCum
			lo, hi := prevBound, b
			if span == 0 {
				return hi
			}
			frac := (target - float64(prevCum)) / float64(span)
			return lo + (hi-lo)*frac
		}
		prevBound, prevCum = b, cum
	}
	return latBounds[len(latBounds)-1]
}

func LatencyCount() uint64 {
	latMu.Lock()
	defer latMu.Unlock()
	return latCount
}

func Handler(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	b.WriteString("# HELP http_requests_total Total HTTP requests by method, route and status code.\n")
	b.WriteString("# TYPE http_requests_total counter\n")
	keys := make([]string, 0)
	httpReqs.Range(func(k, _ any) bool {
		keys = append(keys, k.(string))
		return true
	})
	sort.Strings(keys)
	for _, k := range keys {
		v, _ := httpReqs.Load(k)
		parts := strings.SplitN(k, " ", 3)
		b.WriteString(fmt.Sprintf("http_requests_total{method=%q,route=%q,code=%q} %d\n",
			parts[0], parts[1], parts[2], atomic.LoadUint64(v.(*uint64))))
	}

	b.WriteString("# HELP http_request_duration_seconds Request latency.\n")
	b.WriteString("# TYPE http_request_duration_seconds histogram\n")
	latMu.Lock()
	var cum uint64
	for i, bd := range latBounds {
		cum += latBucket[i]
		b.WriteString(fmt.Sprintf("http_request_duration_seconds_bucket{le=%q} %d\n",
			strconv.FormatFloat(bd, 'f', -1, 64), cum))
	}
	b.WriteString(fmt.Sprintf("http_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", latCount))
	b.WriteString(fmt.Sprintf("http_request_duration_seconds_count %d\n", latCount))
	b.WriteString(fmt.Sprintf("http_request_duration_seconds_sum %s\n", strconv.FormatFloat(latSum, 'f', -1, 64)))
	latMu.Unlock()

	b.WriteString("# HELP wallets_created_total Wallets created by get-or-create.\n")
	b.WriteString("# TYPE wallets_created_total counter\n")
	fmt.Fprintf(&b, "wallets_created_total %d\n", WalletsCreated.Load())
	b.WriteString("# HELP transfers_created_total Transfers completed.\n")
	b.WriteString("# TYPE transfers_created_total counter\n")
	fmt.Fprintf(&b, "transfers_created_total %d\n", TransfersCreated.Load())
	b.WriteString("# HELP transfers_declined_insufficient_funds_total Transfers declined for insufficient funds.\n")
	b.WriteString("# TYPE transfers_declined_insufficient_funds_total counter\n")
	fmt.Fprintf(&b, "transfers_declined_insufficient_funds_total %d\n", TransfersDeclined.Load())
	b.WriteString("# HELP idempotent_replays_total Idempotency-key replays that returned the original result.\n")
	b.WriteString("# TYPE idempotent_replays_total counter\n")
	fmt.Fprintf(&b, "idempotent_replays_total %d\n", IdempotentReplays.Load())

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(b.String()))
}

func StatsHTML() string {
	return fmt.Sprintf(`<!doctype html><html><head><title>wallet stats</title></head><body>
<h1>Wallet service stats</h1>
<ul>
<li>uptime: %s</li>
<li>wallets_created_total: %d</li>
<li>transfers_created_total: %d</li>
<li>transfers_declined_insufficient_funds_total: %d</li>
<li>idempotent_replays_total: %d</li>
<li>http requests: %d</li>
<li>latency p50: %.3f ms</li>
<li>latency p99: %.3f ms</li>
</ul>
<p><a href="/metrics">/metrics</a></p>
</body></html>`,
		time.Since(startTime).Round(time.Second),
		WalletsCreated.Load(),
		TransfersCreated.Load(),
		TransfersDeclined.Load(),
		IdempotentReplays.Load(),
		LatencyCount(),
		Quantile(0.50)*1000,
		Quantile(0.99)*1000)
}

package logs

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	defaultIngestURL = "https://s2755158.us-west-2a.betterstackdata.com"
	chanCap          = 10000
	flushInterval    = 2 * time.Second
	batchSize        = 50
)

type Logger struct {
	ship *shipper
}

func New(sourceToken, ingestURL string) *Logger {
	if ingestURL == "" {
		ingestURL = defaultIngestURL
	}
	l := &Logger{}
	if sourceToken != "" {
		l.ship = newShipper(sourceToken, ingestURL)
	}
	return l
}

func (l *Logger) Close() {
	if l.ship != nil {
		l.ship.Close()
	}
}

func (l *Logger) Event(corrID, event, msg string, fields map[string]any) {
	l.write(map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"level":   "info",
		"corr_id": corrID,
		"event":   event,
		"msg":     msg,
		"fields":  fields,
	})
}

func (l *Logger) Request(corrID, method, path string, status int, durMS float64) {
	lvl := "info"
	if status >= 500 {
		lvl = "warn"
	}
	l.write(map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"level":   lvl,
		"corr_id": corrID,
		"event":   "http_request",
		"msg":     "request completed",
		"method":  method,
		"path":    path,
		"status":  status,
		"dur_ms":  durMS,
		"fields":  map[string]any{},
	})
}

func (l *Logger) Error(corrID, msg string, fields map[string]any) {
	l.write(map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"level":   "error",
		"corr_id": corrID,
		"msg":     msg,
		"fields":  fields,
	})
}

func (l *Logger) write(entry map[string]any) {
	buf, err := json.Marshal(entry)
	if err != nil {
		return
	}
	os.Stdout.Write(append(buf, '\n'))
	if l.ship != nil {
		l.ship.enqueue(entry)
	}
}

type shipper struct {
	token  string
	url    string
	client *http.Client
	ch     chan map[string]any
	done   chan struct{}
	wg     sync.WaitGroup

	mu      sync.Mutex
	dropped uint64
}

func newShipper(token, url string) *shipper {
	s := &shipper{
		token:  token,
		url:    url,
		client: &http.Client{Timeout: 10 * time.Second},
		ch:     make(chan map[string]any, chanCap),
		done:   make(chan struct{}),
	}
	s.wg.Add(1)
	go s.run()
	return s
}

func (s *shipper) Close() {
	close(s.done)
	s.wg.Wait()
}

func (s *shipper) enqueue(entry map[string]any) {
	select {
	case s.ch <- entry:
	default:
		s.mu.Lock()
		s.dropped++
		s.mu.Unlock()
	}
}

func (s *shipper) run() {
	defer s.wg.Done()
	buf := make([]map[string]any, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	flush := func() {
		if len(buf) == 0 {
			return
		}
		s.post(buf)
		buf = buf[:0]
	}
	for {
		select {
		case <-s.done:
			for {
				select {
				case e := <-s.ch:
					buf = append(buf, e)
				default:
					flush()
					return
				}
			}
		case e := <-s.ch:
			buf = append(buf, e)
			if len(buf) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (s *shipper) post(entries []map[string]any) {
	payload := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		payload = append(payload, map[string]any{
			"dt":      time.Now().UnixMilli(),
			"message": e["msg"],
			"level":   e["level"],
			"corr_id": e["corr_id"],
			"event":   e["event"],
			"fields":  e["fields"],
			"ts":      e["ts"],
		})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, s.url, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Authorization", "Bearer "+s.token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.client.Do(req)
		if err == nil && resp.StatusCode < 300 {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
	}
	s.mu.Lock()
	s.dropped += uint64(len(entries))
	s.mu.Unlock()
}

func (s *shipper) Dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

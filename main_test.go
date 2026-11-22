package main

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestValidateAccounts(t *testing.T) {
	tests := []struct {
		name     string
		accounts []SMTPAccount
		wantErr  bool
	}{
		{
			name:     "empty slice",
			accounts: nil,
			wantErr:  true,
		},
		{
			name: "zero limit_per_hour rejected",
			accounts: []SMTPAccount{
				{Host: "smtp.example.com", Port: "587", User: "a@example.com", LimitPerHour: 0},
			},
			wantErr: true,
		},
		{
			name: "negative limit_per_hour rejected",
			accounts: []SMTPAccount{
				{Host: "smtp.example.com", Port: "587", User: "a@example.com", LimitPerHour: -10},
			},
			wantErr: true,
		},
		{
			name: "invalid port rejected",
			accounts: []SMTPAccount{
				{Host: "smtp.example.com", Port: "notaport", User: "a@example.com", LimitPerHour: 10},
			},
			wantErr: true,
		},
		{
			name: "missing host rejected",
			accounts: []SMTPAccount{
				{Host: "", Port: "587", User: "a@example.com", LimitPerHour: 10},
			},
			wantErr: true,
		},
		{
			name: "valid account accepted",
			accounts: []SMTPAccount{
				{Host: "smtp.example.com", Port: "587", User: "a@example.com", LimitPerHour: 50},
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAccounts(tc.accounts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateAccounts() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestWorkerFirstEmailImmediateAndPaced(t *testing.T) {
	origSend := sendEmailFn
	origWindow := rateWindow
	t.Cleanup(func() {
		sendEmailFn = origSend
		rateWindow = origWindow
	})

	// 100ms window with LimitPerHour=1 => 100ms per email.
	// First email must NOT wait 100ms; second email MUST wait ~100ms.
	rateWindow = 100 * time.Millisecond

	var times []time.Time
	var mu sync.Mutex
	sendEmailFn = func(acc SMTPAccount, job EmailJob) error {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		return nil
	}

	acc := SMTPAccount{Host: "smtp.local", Port: "587", User: "sender@local", LimitPerHour: 1}
	h := &workerHandle{queue: make(chan EmailJob, 4), user: acc.User}
	shutdownCh := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		worker(acc, h, shutdownCh)
	}()

	start := time.Now()
	h.wg.Add(2)
	h.queue <- EmailJob{To: "u1@local", Subject: "s1", Content: "c1", RetriesLeft: 1}
	h.queue <- EmailJob{To: "u2@local", Subject: "s2", Content: "c2", RetriesLeft: 1}

	h.wg.Wait()
	close(shutdownCh)
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("expected 2 sends, got %d", len(times))
	}
	firstLatency := times[0].Sub(start)
	if firstLatency >= 50*time.Millisecond {
		t.Fatalf("expected immediate first send (<50ms), took %v", firstLatency)
	}
	gap := times[1].Sub(times[0])
	if gap < 80*time.Millisecond {
		t.Fatalf("expected second send paced by ~100ms, gap was %v", gap)
	}
}

func TestWorkerShutdownRetryNoPanic(t *testing.T) {
	origSend := sendEmailFn
	origWindow := rateWindow
	t.Cleanup(func() {
		sendEmailFn = origSend
		rateWindow = origWindow
	})

	rateWindow = 10 * time.Millisecond

	var attempts atomic.Int32
	inFirstAttempt := make(chan struct{})
	releaseFirstAttempt := make(chan struct{})

	sendEmailFn = func(acc SMTPAccount, job EmailJob) error {
		n := attempts.Add(1)
		if n == 1 {
			close(inFirstAttempt)
			<-releaseFirstAttempt
			return errors.New("transient smtp error")
		}
		return nil
	}

	acc := SMTPAccount{Host: "smtp.local", Port: "587", User: "sender@local", LimitPerHour: 10}
	h := &workerHandle{queue: make(chan EmailJob, 4), user: acc.User}
	shutdownCh := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		worker(acc, h, shutdownCh)
	}()

	h.wg.Add(1)
	h.queue <- EmailJob{To: "u1@local", Subject: "s1", Content: "c1", RetriesLeft: 3}

	// Wait until worker is mid-send, then trigger graceful shutdown.
	<-inFirstAttempt
	close(shutdownCh)
	// Release the failed send so it retries AFTER shutdownCh is closed.
	close(releaseFirstAttempt)

	h.wg.Wait()
	<-done

	if got := attempts.Load(); got != 2 {
		t.Fatalf("expected 2 attempts (1 fail + 1 retry), got %d", got)
	}
}

func TestHandleSendValidationAndFanout(t *testing.T) {
	origWorkers := workers
	origMaxRetries := maxRetries
	t.Cleanup(func() {
		workers = origWorkers
		maxRetries = origMaxRetries
	})

	maxRetries = 2
	h := &workerHandle{queue: make(chan EmailJob, 2), user: "sender@local"}
	workers = []*workerHandle{h}

	// Non-POST rejected
	rec := httptest.NewRecorder()
	handleSend(rec, httptest.NewRequest(http.MethodGet, "/send", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}

	// Missing fields rejected
	rec = httptest.NewRecorder()
	handleSend(rec, httptest.NewRequest(http.MethodPost, "/send", bytes.NewBufferString(`{"destination":[],"subject":"a","content":"b"}`)))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}

	// Valid fanout fills queue of size 2 -> 202
	rec = httptest.NewRecorder()
	handleSend(rec, httptest.NewRequest(http.MethodPost, "/send", bytes.NewBufferString(`{"destination":["a@x.com","b@x.com"],"subject":"Alert","content":"Hi"}`)))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if len(h.queue) != 2 {
		t.Fatalf("expected 2 queued jobs, got %d", len(h.queue))
	}

	// Queue full -> 503
	rec = httptest.NewRecorder()
	handleSend(rec, httptest.NewRequest(http.MethodPost, "/send", bytes.NewBufferString(`{"destination":["c@x.com"],"subject":"Alert","content":"Hi"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when full, got %d", rec.Code)
	}
}

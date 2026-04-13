// Package main implements MailGopher, a lightweight HTTP microservice for
// queuing and sending plain-text emails over SMTP.
//
// The service accepts JSON payloads on POST /send, distributes jobs evenly
// across one dedicated goroutine per configured SMTP account, and paces each
// account's outgoing traffic to stay within its hourly rate limit. Failed
// deliveries are automatically retried up to [maxRetries] times before being
// permanently dropped. On SIGTERM/SIGINT the HTTP listener is closed first so
// no new jobs are accepted, then the service waits for every in-flight and
// queued job to finish before exiting.
//
// Configuration is done entirely through environment variables; see the README
// for the full reference.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gopkg.in/gomail.v2"
)

// EmailRequest is the JSON body expected by the POST /send endpoint.
// All three fields are required; omitting any one returns 400 Bad Request.
// It is decoded from the HTTP request and then fanned out into one [EmailJob]
// per address before being placed on a worker queue.
type EmailRequest struct {
	// Destination holds one or more recipient addresses. Each address is
	// converted into a separate [EmailJob] by [handleSend].
	Destination []string `json:"destination"`

	// Subject is the email subject line.
	Subject string `json:"subject"`

	// Content is the plain-text email body.
	Content string `json:"content"`
}

// EmailJob is the internal unit of work that travels through a worker queue.
// Unlike [EmailRequest], it always carries exactly one recipient address,
// making the single-send invariant explicit in the type rather than a comment.
type EmailJob struct {
	// To is the single recipient address for this send attempt.
	To string

	// Subject is the email subject line, copied from the originating [EmailRequest].
	Subject string

	// Content is the plain-text email body, copied from the originating [EmailRequest].
	Content string

	// RetriesLeft tracks how many send attempts remain before this job is
	// permanently dropped. It is set to [maxRetries] on creation and
	// decremented on each failure.
	RetriesLeft int
}

// SMTPAccount describes a single outgoing SMTP credential set together with
// its hourly send budget. It is decoded from the SMTP_ACCOUNTS environment
// variable, which must be a JSON array of these objects.
type SMTPAccount struct {
	// Host is the SMTP server hostname (e.g. "smtp.gmail.com").
	Host string `json:"host"`

	// Port is the SMTP server port as a string (e.g. "587").
	Port string `json:"port"`

	// User is the SMTP login username, also used as the From address.
	User string `json:"user"`

	// Pass is the SMTP login password.
	Pass string `json:"pass"`

	// LimitPerHour is the maximum number of emails this account may send per
	// hour. The worker derived from this account will pace itself accordingly.
	LimitPerHour int `json:"limit_per_hour"`

	// SSL controls whether the connection is opened with implicit TLS (true)
	// or with a plain connection that is upgraded via STARTTLS (false).
	// Use true for port 465 (SMTPS) and false for port 587 (STARTTLS).
	// Defaults to false when omitted from the JSON configuration.
	SSL bool `json:"ssl"`
}

// workerHandle groups the per-account job channel with the WaitGroup that
// tracks its outstanding jobs. Keeping them together avoids passing both
// separately throughout the codebase and makes shutdown logic self-contained.
type workerHandle struct {
	// queue is the dedicated, bounded channel through which jobs are delivered
	// to this worker. It is closed during graceful shutdown to signal the
	// worker goroutine to drain and exit.
	queue chan EmailJob

	// wg counts jobs that have been accepted into the queue but not yet
	// resolved (either sent successfully or permanently failed). The main
	// goroutine waits on wg during shutdown to ensure nothing is dropped.
	wg sync.WaitGroup

	// user is the SMTP username associated with this handle, stored here for
	// use in log messages without needing to pass SMTPAccount around.
	user string
}

// workers holds one handle per configured SMTP account. It is written once
// during startup and then read concurrently by dispatch, so no mutex is needed.
var workers []*workerHandle

// robin is an atomically incremented counter used by dispatch to assign jobs
// to workers in a round-robin fashion without requiring a lock.
var robin atomic.Uint64

// maxRetries is the number of times a failed send is retried before the job
// is permanently dropped. It is set once during startup from MAX_RETRIES.
var maxRetries int

// setupLogger initialises the global slog logger from LOG_LEVEL and LOG_FORMAT
// environment variables. It must be called before any other logging takes place.
//
// LOG_LEVEL accepts DEBUG, INFO (default), WARN/WARNING, or ERROR.
// LOG_FORMAT accepts "text" for human-readable output or "json" (default) for
// structured machine-readable output.
func setupLogger() {
	var level slog.Level
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN", "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.ToLower(os.Getenv("LOG_FORMAT")) == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(handler))
}

// main is the service entry point. It parses configuration, starts one worker
// goroutine per SMTP account, starts the HTTP server, and blocks until a
// SIGTERM or SIGINT is received. On shutdown it drains all queues before
// returning.
func main() {
	setupLogger()

	accountsEnv := os.Getenv("SMTP_ACCOUNTS")
	if accountsEnv == "" {
		slog.Error("SMTP_ACCOUNTS environment variable is not set")
		os.Exit(1)
	}

	var accounts []SMTPAccount
	if err := json.Unmarshal([]byte(accountsEnv), &accounts); err != nil {
		slog.Error("Failed to parse SMTP_ACCOUNTS JSON", "error", err)
		os.Exit(1)
	}
	if len(accounts) == 0 {
		slog.Error("No SMTP accounts configured")
		os.Exit(1)
	}

	maxRetries = 3
	if retriesEnv := os.Getenv("MAX_RETRIES"); retriesEnv != "" {
		if parsed, err := strconv.Atoi(retriesEnv); err == nil && parsed >= 0 {
			maxRetries = parsed
		} else {
			slog.Warn("Invalid MAX_RETRIES value, using default", "default", maxRetries, "provided", retriesEnv)
		}
	}

	slog.Info("Configuration loaded", "max_retries", maxRetries, "account_count", len(accounts))

	workerQueueSize := os.Getenv("WORKER_QUEUE_SIZE")
	if workerQueueSize == "" {
		workerQueueSize = "100"
	}
	queueSize, err := strconv.Atoi(workerQueueSize)
	if err != nil {
		slog.Error("Invalid WORKER_QUEUE_SIZE value", "error", err)
		os.Exit(1)
	}
	if queueSize < 1 {
		queueSize = 1
	}

	workers = make([]*workerHandle, len(accounts))
	for i, acc := range accounts {
		h := &workerHandle{
			queue: make(chan EmailJob, queueSize),
			user:  acc.User,
		}
		workers[i] = h
		go worker(acc, h)
		slog.Info("Started worker", "user", acc.User, "limit_per_hour", acc.LimitPerHour, "queue_size", queueSize)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/send", handleSend)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		slog.Info("Email service listening", "port", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server error", "error", err)
			os.Exit(1)
		}
	}()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	<-stopChan

	slog.Info("Shutdown signal received. Stopping HTTP server to reject new requests...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	// Closing each channel signals the corresponding worker goroutine to exit
	// its range loop once the queue is fully drained.
	for _, h := range workers {
		close(h.queue)
	}

	slog.Info("HTTP server stopped. Waiting for all per-worker queues to drain...")
	for _, h := range workers {
		h.wg.Wait()
	}
	slog.Info("All queued emails processed. Service exiting gracefully.")
}

// dispatch places req into the queue of the next worker selected by
// round-robin. If that worker's queue is full it tries each remaining worker
// in order before giving up.
//
// It returns true when the job was accepted and false when every worker queue
// is full, indicating the caller should respond with 503 Service Unavailable.
// When true is returned, the relevant worker's WaitGroup is incremented so
// the job is tracked for graceful shutdown.
func dispatch(req EmailJob) bool {
	n := uint64(len(workers))
	start := robin.Add(1) - 1
	for i := uint64(0); i < n; i++ {
		h := workers[(start+i)%n]
		select {
		case h.queue <- req:
			h.wg.Add(1)
			return true
		default:
			// This worker's queue is full; fall through to try the next one.
		}
	}
	return false
}

// handleSend is the HTTP handler for POST /send. It decodes an [EmailRequest]
// from the JSON body, validates that all required fields are present, and
// fans the request out into one individual job per destination address so that
// each send counts as exactly one outgoing email on the remote SMTP server.
//
// If any individual job cannot be queued (all worker queues full) the handler
// stops immediately and responds with 503; jobs that were already enqueued in
// that same request are not rolled back.
//
// Possible responses:
//   - 202 Accepted            - all per-destination jobs successfully queued.
//   - 400 Bad Request         - malformed JSON or missing required fields.
//   - 405 Method Not Allowed  - request was not a POST.
//   - 503 Service Unavailable - a worker queue is full mid-fanout.
func handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EmailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}
	if len(req.Destination) == 0 || req.Subject == "" || req.Content == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Fan out: convert each destination address into its own EmailJob so the
	// remote SMTP server counts every send individually.
	for _, addr := range req.Destination {
		job := EmailJob{
			To:          addr,
			Subject:     req.Subject,
			Content:     req.Content,
			RetriesLeft: maxRetries,
		}
		if !dispatch(job) {
			slog.Warn("Service unavailable: all worker queues are full")
			http.Error(w, "Service unavailable: queue is full", http.StatusServiceUnavailable)
			return
		}
	}

	slog.Debug("Jobs successfully queued", "subject", req.Subject, "destinations", len(req.Destination))
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("ok\n"))
}

// worker is the per-account send loop. It exclusively consumes jobs from
// h.queue and paces each send attempt with a time.Ticker derived from
// acc.LimitPerHour, ensuring the hourly rate limit is never exceeded.
//
// On send failure the job's RetriesLeft counter is decremented and the job is
// re-enqueued into the same worker's queue via a non-blocking select so that
// retry attempts consume budget from the correct account. If the queue is full
// at retry time, or if RetriesLeft reaches zero, the job is permanently
// dropped and h.wg.Done is called.
//
// worker exits cleanly when h.queue is closed (during graceful shutdown) after
// the range loop finishes draining all remaining jobs.
func worker(acc SMTPAccount, h *workerHandle) {
	interval := time.Hour / time.Duration(acc.LimitPerHour)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for job := range h.queue {
		<-ticker.C

		err := sendEmail(acc, job)
		if err != nil {
			job.RetriesLeft--
			if job.RetriesLeft > 0 {
				slog.Warn("Failed to send email, requeuing",
					"worker", acc.User,
					"subject", job.Subject,
					"retries_left", job.RetriesLeft,
					"error", err.Error())

				select {
				case h.queue <- job:
					// Re-queued successfully; WaitGroup counter remains open.
				default:
					// Queue is full or closed during shutdown — drop the job.
					slog.Error("Could not requeue email, dropping",
						"worker", acc.User,
						"subject", job.Subject,
						"retries_left", job.RetriesLeft)
					h.wg.Done()
				}
			} else {
				slog.Error("Permanently failed to send email after max retries",
					"worker", acc.User,
					"subject", job.Subject,
					"error", err.Error())
				h.wg.Done()
			}
		} else {
			slog.Info("Email sent successfully",
				"worker", acc.User,
				"subject", job.Subject,
				"to", job.To)
			h.wg.Done()
		}
	}
}

// sendEmail constructs a gomail message from job and delivers it through the
// SMTP credentials in acc using a fresh connection per call.
//
// job.Destination is always a single address (guaranteed by the fan-out in
// [handleSend]), set directly as the To header so the remote SMTP server
// counts it as exactly one outgoing email.
//
// The dialler is configured for implicit TLS when acc.SSL is true (port 465),
// or a plain connection upgraded via STARTTLS when false (port 587). The SSL
// field defaults to false, so existing configurations that omit it are
// unaffected.
//
// It returns the first error encountered during dialling or sending, or nil on
// success.
func sendEmail(acc SMTPAccount, job EmailJob) error {
	port, err := strconv.Atoi(acc.Port)
	if err != nil {
		return fmt.Errorf("invalid SMTP port %q: %w", acc.Port, err)
	}

	name := os.Getenv("NAME")
	if name == "" {
		name = acc.User
	}

	m := gomail.NewMessage()
	m.SetHeader("From", m.FormatAddress(acc.User, name))
	m.SetHeader("To", job.To)
	m.SetHeader("Subject", job.Subject)
	m.SetBody("text/plain", job.Content)

	d := gomail.NewDialer(acc.Host, port, acc.User, acc.Pass)
	d.SSL = acc.SSL
	return d.DialAndSend(m)
}

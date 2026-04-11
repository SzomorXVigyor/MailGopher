# MailGopher - An Email Sender Microservice

A lightweight microservice for queuing and sending plain-text emails over SMTP, written in Go. It accepts HTTP requests, fans them out into individual per-recipient jobs, and processes each job in the background across multiple SMTP accounts - respecting strict hourly rate limits, handling automatic retries on failure, and shutting down gracefully without data loss.

## Features

- **Per-recipient sending:** each address in a request is dispatched as its own individual job so the remote SMTP server counts every send exactly once, avoiding inflated outgoing counts from BCC grouping.
- **Per-account worker queues:** every configured SMTP account gets its own dedicated goroutine and bounded queue. Jobs are distributed via round-robin dispatch, so rate limits are deterministic and accounts never compete for the same channel.
- **Strict per-account hourly rate limiting:** each worker paces sends using a ticker derived from `limit_per_hour`, spacing outgoing traffic evenly over the hour.
- **SSL / STARTTLS support:** each account can independently use implicit TLS (port 465) or STARTTLS (port 587).
- **Automatic retry:** failed deliveries are re-queued into the same worker's queue and retried up to `MAX_RETRIES` times before being permanently dropped.
- **Graceful shutdown:** on SIGTERM/SIGINT the HTTP listener closes first (no new requests accepted), then the service waits for every queued and in-flight job to finish before exiting.

## Configuration

The service is configured entirely via environment variables.

| Variable        | Default | Description                                                     |
| --------------- | ------- | --------------------------------------------------------------- |
| `PORT`          | `8080`  | HTTP port to listen on.                                         |
| `LOG_LEVEL`     | `INFO`  | Log severity. Options: `DEBUG`, `INFO`, `WARN`, `ERROR`.        |
| `LOG_FORMAT`    | `json`  | Log output format. Options: `json`, `text`.                     |
| `MAX_RETRIES`   | `3`     | Number of times to retry a failed send before dropping the job. |
| `SMTP_ACCOUNTS` | -       | **Required.** JSON array of SMTP account objects (see below).   |

### SMTP Account Object

| Field            | Type    | Description                                                                               |
| ---------------- | ------- | ----------------------------------------------------------------------------------------- |
| `host`           | string  | SMTP server hostname (e.g. `smtp.gmail.com`).                                             |
| `port`           | string  | SMTP server port (e.g. `"587"` or `"465"`).                                               |
| `user`           | string  | SMTP login username, also used as the `From` address.                                     |
| `pass`           | string  | SMTP login password.                                                                      |
| `limit_per_hour` | number  | Maximum emails this account may send per hour.                                            |
| `ssl`            | boolean | `true` for implicit TLS / port 465, `false` for STARTTLS / port 587. Defaults to `false`. |

### Example - Envs

```bash
export LOG_FORMAT=text
export LOG_LEVEL=DEBUG
export MAX_RETRIES=5
export SMTP_ACCOUNTS='[
  {
    "host": "smtp.gmail.com",
    "port": "587",
    "user": "sender1@domain.com",
    "pass": "password_here",
    "limit_per_hour": 50,
    "ssl": false
  },
  {
    "host": "smtp.example.com",
    "port": "465",
    "user": "sender2@domain.com",
    "pass": "password_here",
    "limit_per_hour": 100,
    "ssl": true
  }
]'
```

## API

### POST /send

Queues an email for sending. Each address in `destination` is enqueued as a separate job, so a request with three addresses consumes three slots from the per-account rate limit budget.

**Request Body**

```json
{
    "destination": ["user1@example.com", "user2@example.com"],
    "subject": "System Alert",
    "content": "This is a plain text notification."
}
```

**Responses**

| Status                    | Meaning                                                                          |
| ------------------------- | -------------------------------------------------------------------------------- |
| `202 Accepted`            | All per-recipient jobs successfully queued. Response body: `ok`.                 |
| `400 Bad Request`         | Invalid JSON or one or more required fields are missing.                         |
| `405 Method Not Allowed`  | Request method was not POST.                                                     |
| `503 Service Unavailable` | A worker queue is full. Total buffer is ~5000 jobs split evenly across accounts. |

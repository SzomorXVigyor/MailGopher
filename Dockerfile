# ==========================================
# Stage 1: Build Stage
# ==========================================
FROM golang:1.26.2-alpine3.23 AS builder

# Set the working directory inside the container
WORKDIR /app

# Install root CA certificates. 
# This is CRITICAL because the app needs to verify TLS/SSL certificates 
# when connecting to external SMTP servers (like Gmail or Mailgun).
RUN apk --no-cache add ca-certificates

# Copy go.mod and go.sum to download dependencies.
# (Doing this before copying the rest of the code caches the downloaded modules)
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build a fully static Go binary.
# CGO_ENABLED=0 disables C dependencies, making it run perfectly on scratch.
# -ldflags="-w -s" strips debugging information to make the binary even smaller.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o mailer-service main.go

# ==========================================
# Stage 2: Final Minimal Image
# ==========================================
FROM scratch

# Copy the CA certificates from the builder stage so TLS connections work
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the compiled, statically-linked binary from the builder stage
COPY --from=builder /app/mailer-service /mailer-service

# Expose the default port (matches the default PORT env var)
EXPOSE 8080

# Command to run the executable
ENTRYPOINT ["/mailer-service"]
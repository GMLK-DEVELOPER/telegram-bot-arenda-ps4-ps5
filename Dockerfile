# ── Build stage ───────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o ps4-rental ./cmd/main.go

# ── Final stage ───────────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/ps4-rental .
COPY templates/ ./templates/
COPY static/     ./static/

RUN mkdir -p data passport static/img/console

EXPOSE 8080

CMD ["./ps4-rental"]

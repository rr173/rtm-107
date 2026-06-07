FROM golang:1.21-alpine AS builder

WORKDIR /app

RUN apk add --no-cache gcc musl-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o /lock-server ./cmd/lock-server

FROM alpine:latest

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /lock-server /app/lock-server

RUN mkdir -p /app/data

ENV DB_PATH=/app/data/locks.db
ENV ADDR=:8080

EXPOSE 8080

CMD ["/app/lock-server"]

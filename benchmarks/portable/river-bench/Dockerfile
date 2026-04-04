FROM golang:1.25-alpine AS builder
RUN apk add --no-cache git
WORKDIR /app
COPY go.mod main.go ./
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 go build -o /river-bench .

FROM alpine:3.20
COPY --from=builder /river-bench /usr/local/bin/river-bench
ENTRYPOINT ["river-bench"]

FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY . .

RUN go build -o forgenx-engine ./cmd/forgenx-engine

FROM alpine:3.19

WORKDIR /app

COPY --from=builder /app/forgenx-engine /app/forgenx-engine
COPY static ./static

RUN mkdir -p /pool/coins

EXPOSE 3333
EXPOSE 8080

CMD ["/app/forgenx-engine"]

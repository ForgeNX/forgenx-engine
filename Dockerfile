FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY . .

RUN go build -o gostratumengine ./cmd/gostratumengine

FROM alpine:3.19

WORKDIR /app

COPY --from=builder /app/gostratumengine /app/gostratumengine

RUN mkdir -p /pool/coins

EXPOSE 3333
EXPOSE 8080

CMD ["/app/gostratumengine"]

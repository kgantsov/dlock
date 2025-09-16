FROM golang:1.24.7 AS builder


# Copy the code from the host and compile it
WORKDIR $GOPATH/src/github.com/kgantsov/dlock
COPY ./ ./
RUN go mod download
WORKDIR $GOPATH/src/github.com/kgantsov/dlock/
WORKDIR $GOPATH/src/github.com/kgantsov/dlock/cmd/server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app .

FROM alpine:latest as alpine
RUN apk --no-cache add tzdata zip ca-certificates
WORKDIR /usr/share/zoneinfo
# -0 means no compression.  Needed because go's
# tz loader doesn't handle compressed data.
RUN zip -r -0 /zoneinfo.zip .

FROM alpine

ENV ZONEINFO /zoneinfo.zip
COPY --from=alpine /zoneinfo.zip /

COPY --from=builder /app /
COPY --from=alpine /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# CMD ["/app --port 8780"]

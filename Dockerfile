FROM golang:1.17-alpine as builder

WORKDIR /usr/src
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o kype ./cmd/kype

FROM alpine:latest
RUN apk update && apk add --no-cache ca-certificates tzdata

WORKDIR /usr/app
COPY --from=builder /usr/src/kype .
CMD ["./kype"]

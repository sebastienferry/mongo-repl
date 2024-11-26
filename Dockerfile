FROM golang AS builder
WORKDIR /src

COPY . .
RUN go mod download
RUN OS=linux GOARCH=amd64 go build -o /out/repl /src/cmd/repl/main.go

FROM alpine
WORKDIR /app
COPY --from=builder /out/repl .
CMD ["/app/repl"]
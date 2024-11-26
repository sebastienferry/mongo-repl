FROM --platform=$BUILDPLATFORM golang AS builder
WORKDIR /src

COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN go mod download
RUN GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /out/repl /src/cmd/repl/main.go

FROM alpine
WORKDIR /app
COPY --from=builder /out/repl .
CMD ["/app/repl"]
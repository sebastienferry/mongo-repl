build:
    echo "Building..."
    go build -o bin/mongo-repl ./cmd/...

clean:
    echo "Cleaning..."
    rm -rf bin

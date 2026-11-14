#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "==> gofmt"
unformatted="$(gofmt -l .)"
if [[ -n "$unformatted" ]]; then
  echo "Unformatted files:"
  echo "$unformatted"
  exit 1
fi

echo "==> go vet"
go vet ./...

echo "==> go test"
go test -race -count=1 ./...

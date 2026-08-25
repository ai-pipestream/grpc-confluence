#!/usr/bin/env bash
# Generates Go stubs for the Confluence API protos into gen/.
# Needs buf plus protoc-gen-go and protoc-gen-go-grpc on PATH:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail
cd "$(dirname "$0")"
export PATH="$PATH:$(go env GOPATH)/bin"
buf generate ../../grpc-confluence-api/src/main/proto --template buf.gen.yaml
echo "stubs in $(pwd)/gen"

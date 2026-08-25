#!/usr/bin/env bash
# Generates Python stubs for the Confluence API protos into gen/.
set -euo pipefail
cd "$(dirname "$0")"
PROTO_ROOT=../../grpc-confluence-api/src/main/proto
mkdir -p gen
python3 -m grpc_tools.protoc \
  -I "$PROTO_ROOT" \
  --python_out=gen \
  --grpc_python_out=gen \
  "$PROTO_ROOT"/ai/pipestream/confluence/v1/*.proto
# Make the generated tree importable as plain packages.
find gen -type d -exec touch {}/__init__.py \;
echo "stubs in $(pwd)/gen"

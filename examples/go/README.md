# Go example

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
./generate.sh                                  # buf managed mode -> gen/
go mod tidy
CONFLUENCE_GRPC_TARGET=localhost:9095 go run .
```

The API protos carry no `go_package` option; `buf.gen.yaml` supplies one
through managed mode (`confluence-example/gen` prefix), so the generated
packages resolve inside this module without touching the protos.

`main.go` probes the connection, lists spaces, streams one space's pages,
fetches a page body in storage format, and runs a bounded `Sync`,
printing the resume cursor to persist for the next incremental pass.

`gen/` is codegen output - regenerate with `generate.sh` after any proto
change; do not commit it.

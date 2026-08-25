# Java example

The example compiles against the repo's own generated API - no codegen
step, no extra dependencies. From the repo root:

```bash
./gradlew :grpc-confluence-service:installDist
LIB=../../grpc-confluence-service/build/install/grpc-confluence-service/lib
cd examples/java
javac -cp "$LIB/*" ConfluenceExample.java
CONFLUENCE_GRPC_TARGET=localhost:9095 java -cp ".:$LIB/*" ConfluenceExample
```

`ConfluenceExample` probes the connection, lists spaces, streams one
space's pages (server-streaming rpcs surface as an `Iterator` on the
blocking stub), fetches a page body in storage format, and runs a
bounded `Sync`, printing the resume cursor to persist for the next
incremental pass.

In your own build, depend on the `grpc-confluence-api` artifact (or
vendor the protos and generate with `protoc`) plus `grpc-netty-shaded`,
`grpc-protobuf`, and `grpc-stub`.

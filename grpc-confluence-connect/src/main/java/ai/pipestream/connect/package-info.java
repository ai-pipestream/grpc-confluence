/**
 * Kafka Connect source plugin that emits raw ConfluenceChange protobuf
 * bytes, built on the grpc-integration {@code grpc-connect} base classes.
 * It can crawl through the in-process Confluence client or pull a
 * {@code Sync} stream from ConfluenceService.
 */
package ai.pipestream.connect;

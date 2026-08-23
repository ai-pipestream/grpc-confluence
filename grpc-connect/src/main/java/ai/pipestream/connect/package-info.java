/**
 * Kafka Connect source plugins that emit raw ConfluenceChange /
 * MicrosoftChange protobuf bytes. Each connector can crawl through the
 * in-process client or pull a {@code Sync} stream from the matching
 * gRPC service.
 */
package ai.pipestream.connect;

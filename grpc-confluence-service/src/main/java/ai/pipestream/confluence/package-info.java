/**
 * Standalone Confluence Cloud gRPC proxy: REST v2 client, Jackson mapper,
 * virtual-thread crawler, programmatic validator, and a reflection-on
 * Netty facade. Optional {@link ai.pipestream.confluence.KafkaChangeSink}
 * publishes raw protobuf bytes when bootstrap servers are configured.
 * Optional {@link ai.pipestream.confluence.OkfChangeSink} writes an OKF
 * bundle and a sibling WARC 1.1 file when {@code OKF_DIR} is set.
 */
package ai.pipestream.confluence;

/**
 * Swappable crawl-output SPI. {@link ai.pipestream.output.OutputStore}
 * implementations (filesystem, S3, …) are discovered with
 * {@link java.util.ServiceLoader}. {@link ai.pipestream.output.OutputFormat}
 * implementations (protobuf, JSON, OKF markdown, Microsoft connector) write
 * through a store. Filesystem is the default store; callers check
 * {@link ai.pipestream.output.OutputStores#has(String)} to see whether another
 * store jar was loaded.
 */
package ai.pipestream.output;

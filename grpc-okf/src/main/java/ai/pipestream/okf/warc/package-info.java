/**
 * WARC 1.1 (ISO 28500) writer used beside an OKF zip. One {@code resource}
 * (or {@code conversion}) record per URI; the collection HTML is its own
 * resource whose {@code href}s are those {@code WARC-Target-URI}s. The OKF
 * zip is never stored as a WARC payload.
 */
package ai.pipestream.okf.warc;

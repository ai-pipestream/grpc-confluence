/**
 * Open Knowledge Format (OKF) v0.2 producer: markdown concepts with YAML
 * frontmatter, {@code index.md}/{@code log.md}, attested computations,
 * directory bundles, and zip archives; plus a sibling WARC 1.1 file with
 * one {@code resource} record per live URI and {@code conversion} records
 * for the OKF markdown. The ZIP is the OKF distribution form; WARC is the
 * per-URI archive. They are siblings, not nested. See
 * https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md
 * and ISO 28500 / WARC 1.1.
 */
package ai.pipestream.okf;

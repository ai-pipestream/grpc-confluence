package ai.pipestream.okf;

import com.google.protobuf.Message;

import java.util.Optional;

/**
 * Maps one source's change messages onto OKF catalog entries. Each source
 * output module (Confluence, Microsoft Graph, ...) registers one
 * implementation with {@link java.util.ServiceLoader}, so {@code grpc-okf}
 * never depends on a source API.
 */
public interface OkfCatalogSource {

    /**
     * Stable source id, used to group records into one bundle per source.
     *
     * @return the id, for example {@code confluence}
     */
    String id();

    /**
     * Whether this source owns {@code record}.
     *
     * @param record a change message from a crawler
     * @return true when {@link #entry} understands the type
     */
    boolean supports(Message record);

    /**
     * Maps one change.
     *
     * @param record a change this source {@link #supports}
     * @return the catalog entry, or empty when the change carries no entity
     */
    Optional<CatalogEntry> entry(Message record);

    /**
     * Bundle title for a run of this source.
     *
     * @return the title
     */
    String title();

    /**
     * Bundle description for a run of this source.
     *
     * @return the description
     */
    String description();

    /**
     * Actor recorded as the bundle's generator (see {@link OkfActor}).
     *
     * @return the actor
     */
    String actor();

    /**
     * Software identifier recorded on the bundle.
     *
     * @return the producer id, for example {@code grpc-confluence/okf-producer}
     */
    String software();
}

package org.softwareheritage.graph.algo;

import java.util.function.LongConsumer;

public interface NodeIdConsumer extends LongConsumer {

    /** Callback for incrementally receiving node identifiers during a graph
     * visit.
     */
    void accept(long nodeId);
}

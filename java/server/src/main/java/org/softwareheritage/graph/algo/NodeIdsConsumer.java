package org.softwareheritage.graph.algo;

import java.util.function.BiConsumer;

public interface NodeIdsConsumer extends BiConsumer {

    /** Callback for returning a (potentially large) list of node identifiers.
     * The callback will be invoked repeatedly without reallocating the array.
     * At each invocation the array might contain more than size node
     * identifiers, but only identifiers located up to position size-1 are to
     * be considered during that specific invocation.
     */
    void accept(long nodeIds[], int size);
}

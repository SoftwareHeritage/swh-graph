package org.softwareheritage.graph.algo;

public interface EdgeIdConsumer {

    /** Callback for incrementally receiving edge identifiers during a graph
     * visit.
     */
    void accept(long srcId, long dstId);
}

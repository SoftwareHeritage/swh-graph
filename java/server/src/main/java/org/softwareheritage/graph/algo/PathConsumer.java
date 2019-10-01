package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.function.Consumer;

public interface PathConsumer extends Consumer<ArrayList<Long>> {

    /** Callback for incrementally receiving node paths (made of node
     * identifiers) during a graph visit.
     */
    void accept(ArrayList<Long> path);
}

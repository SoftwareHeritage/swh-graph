package org.softwareheritage.graph.benchmark.utils;

import java.util.PrimitiveIterator;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;

/**
 * Random related utility class.
 *
 * @author The Software Heritage developers
 */

public class Random {
    /** Internal pseudorandom generator */
    java.util.Random random;

    /**
     * Constructor.
     */
    public Random() {
        this.random = new java.util.Random();
    }

    /**
     * Constructor.
     *
     * @param seed random generator seed
     */
    public Random(long seed) {
        this.random = new java.util.Random(seed);
    }

    /**
     * Generates random node ids.
     *
     * @param graph graph used to pick node ids
     * @param nbNodes number of node ids to generate
     * @return an array of random node ids
     */
    public long[] generateNodeIds(Graph graph, int nbNodes) {
        return random.longs(nbNodes, 0, graph.getNbNodes()).toArray();
    }

    /**
     * Generates random node ids with a specific type.
     *
     * @param graph graph used to pick node ids
     * @param nbNodes number of node ids to generate
     * @param expectedType specific node type to pick
     * @return an array of random node ids
     */
    public long[] generateNodeIdsOfType(Graph graph, int nbNodes, Node.Type expectedType) {
        PrimitiveIterator.OfLong nodes = random.longs(0, graph.getNbNodes()).iterator();
        long[] nodeIds = new long[nbNodes];

        long nextId;
        for (int i = 0; i < nbNodes; i++) {
            do {
                nextId = nodes.nextLong();
            } while (graph.getNodeType(nextId) != expectedType);
            nodeIds[i] = nextId;
        }

        return nodeIds;
    }
}

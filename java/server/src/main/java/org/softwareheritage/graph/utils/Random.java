package org.softwareheritage.graph.utils;

import org.softwareheritage.graph.Graph;

/**
 * Random related utility class.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
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
   * @param graph graph from which node ids will be picked
   * @param nbNodes number of node ids to generate
   * @return an array of random node ids
   */
  public long[] generateNodeIds(Graph graph, long nbNodes) {
    return random.longs(nbNodes, 0, graph.getNbNodes()).toArray();
  }
}

package org.softwareheritage.graph.backend;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;

import org.softwareheritage.graph.backend.NodeIdMap;

public class Setup {
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Expected parameters: <nodes.csv.gz path> <compressed graph path>");
      System.exit(1);
    }

    String nodesPath = args[0];
    String graphPath = args[1];

    System.out.println("Pre-computing node id maps...");
    long startTime = System.nanoTime();
    precomputeNodeIdMap(nodesPath, graphPath);
    long endTime = System.nanoTime();
    double duration = (double) (endTime - startTime) / 1_000_000_000;
    System.out.println("Done in: " + duration + " seconds");
  }

  // Suppress warning for Object2LongFunction cast
  @SuppressWarnings("unchecked")
  static void precomputeNodeIdMap(String nodesPath, String graphPath) throws IOException {
    // First internal mapping: SWH id (string) -> WebGraph MPH (long)
    Object2LongFunction<String> mphMap = null;
    try {
      mphMap = (Object2LongFunction<String>) BinIO.loadObject(graphPath + ".mph");
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("The .mph file contains unknown class object: " + e);
    }
    long nbIds = (mphMap instanceof Size64) ? ((Size64) mphMap).size64() : mphMap.size();

    // Second internal mapping: WebGraph MPH (long) -> BFS ordering (long)
    long[][] bfsMap = LongBigArrays.newBigArray(nbIds);
    long loaded = BinIO.loadLongs(graphPath + ".order", bfsMap);
    if (loaded != nbIds) {
      throw new IllegalArgumentException("Graph contains " + nbIds + " nodes, but read " + loaded);
    }

    // Dump complete mapping for all nodes: SWH id (string) <=> WebGraph node id (long)

    InputStream nodesStream = new GZIPInputStream(new FileInputStream(nodesPath));
    FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(nodesStream, "UTF-8"));
    LineIterator swhIdIterator = new LineIterator(buffer);

    try (
        Writer swhToNodeMap = new BufferedWriter(new FileWriter(graphPath + ".swhToNodeMap.csv"));
        Writer nodeToSwhMap = new BufferedWriter(new FileWriter(graphPath + ".nodeToSwhMap.csv"))) {
      // nodeToSwhMap needs to write SWH id in order of node id, so use a temporary array
      Object[][] nodeToSwhId = ObjectBigArrays.newBigArray(nbIds);

      for (long iNode = 0; iNode < nbIds && swhIdIterator.hasNext(); iNode++) {
        String swhId = swhIdIterator.next().toString();
        long mphId = mphMap.getLong(swhId);
        long nodeId = LongBigArrays.get(bfsMap, mphId);

        String paddedNodeId = String.format("%0" + NodeIdMap.NODE_ID_LENGTH + "d", nodeId);
        String line = swhId + " " + paddedNodeId + "\n";
        swhToNodeMap.write(line);

        ObjectBigArrays.set(nodeToSwhId, nodeId, swhId);
      }

      for (long iNode = 0; iNode < nbIds; iNode++) {
        String line = ObjectBigArrays.get(nodeToSwhId, iNode).toString() + "\n";
        nodeToSwhMap.write(line);
      }
    }
  }
}

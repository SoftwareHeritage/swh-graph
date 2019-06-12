package org.softwareheritage.graph.backend;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;

import org.softwareheritage.graph.backend.NodeIdMap;
import org.softwareheritage.graph.backend.utils.MMapOutputFile;

public class Setup {
  public static void main(String[] args) throws IOException {
    String graphPath = args[0];

    System.out.println("Pre-computing node id maps...");
    long startTime = System.nanoTime();
    precomputeNodeIdMap(graphPath);
    long endTime = System.nanoTime();
    double duration = (double) (endTime - startTime) / 1_000_000_000;
    System.out.println("Done in: " + duration + " seconds");
  }

  // Suppress warning for Object2LongFunction cast
  @SuppressWarnings("unchecked")
  static void precomputeNodeIdMap(String graphPath) throws IOException {
    // First internal mapping: SWH id (string) -> WebGraph MPH (long)
    Object2LongFunction<String> mphMap = null;
    try {
      mphMap = (Object2LongFunction<String>) BinIO.loadObject(graphPath + ".mph");
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("The .mph file contains unknown class object: " + e);
    }
    long nbIds = mphMap.size();

    // Second internal mapping: WebGraph MPH (long) -> BFS ordering (long)
    long[][] bfsMap = LongBigArrays.newBigArray(nbIds);
    long loaded = BinIO.loadLongs(graphPath + ".order", bfsMap);
    if (loaded != nbIds) {
      throw new IllegalArgumentException("Graph contains " + nbIds + " nodes, but read " + loaded);
    }

    // Dump complete mapping for all nodes: SWH id (string) <=> WebGraph node id (long)
    MMapOutputFile swhToNodeMap = new MMapOutputFile(
        graphPath + ".swhToNodeMap.csv", NodeIdMap.SWH_TO_NODE_LINE_LENGTH, nbIds);
    MMapOutputFile nodeToSwhMap = new MMapOutputFile(
        graphPath + ".nodeToSwhMap.csv", NodeIdMap.NODE_TO_SWH_LINE_LENGTH, nbIds);

    InputStream nodeFile = new GZIPInputStream(new FileInputStream(graphPath + ".nodes.csv.gz"));
    FastBufferedReader fileBuffer =
        new FastBufferedReader(new InputStreamReader(nodeFile, "UTF-8"));
    LineIterator lineIterator = new LineIterator(fileBuffer);

    for (long iNode = 0; iNode < nbIds && lineIterator.hasNext(); iNode++) {
      String swhId = lineIterator.next().toString();
      long mphId = mphMap.getLong(swhId);
      long nodeId = LongBigArrays.get(bfsMap, mphId);

      {
        String paddedNodeId = String.format("%0" + NodeIdMap.NODE_ID_LENGTH + "d", nodeId);
        String line = swhId + " " + paddedNodeId + "\n";
        long lineIndex = iNode;
        swhToNodeMap.writeAtLine(line, lineIndex);
      }

      {
        String line = swhId + "\n";
        long lineIndex = nodeId;
        nodeToSwhMap.writeAtLine(line, lineIndex);
      }
    }

    swhToNodeMap.close();
    nodeToSwhMap.close();
  }
}

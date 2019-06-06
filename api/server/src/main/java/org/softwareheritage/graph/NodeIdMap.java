package org.softwareheritage.graph;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;

import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.utils.MMapInputFile;
import org.softwareheritage.graph.utils.MMapOutputFile;

// TODO:
//  - Add option to dump or not the mapping
//  - Add error handling when node/swh ids not found

public class NodeIdMap {
  private static final int SWH_ID_SIZE = 50;
  private static final int NODE_ID_SIZE = 20;
  // +1 are for spaces and end of lines
  private static final int SWH_TO_NODE_LINE_LENGTH = SWH_ID_SIZE + 1 + NODE_ID_SIZE + 1;
  private static final int NODE_TO_SWH_LINE_LENGTH = SWH_ID_SIZE + 1;

  String graphPath;
  long nbIds;
  MMapInputFile swhToNodeMap;
  MMapInputFile nodeToSwhMap;

  public NodeIdMap(String graphPath, long nbNodes) {
    this.graphPath = graphPath;
    this.nbIds = nbNodes;

    try {
      dump();
    } catch (Exception e) {
      System.out.println("Could not dump mapping: " + e);
    }

    this.swhToNodeMap = new MMapInputFile(graphPath + ".swhToNodeMap.csv", SWH_TO_NODE_LINE_LENGTH);
    this.nodeToSwhMap = new MMapInputFile(graphPath + ".nodeToSwhMap.csv", NODE_TO_SWH_LINE_LENGTH);
  }

  // SWH id (string) -> WebGraph node id (long)
  // Each line in .swhToNode.csv is formatted as: swhId nodeId
  // The file is sorted by swhId, hence we can binary search on swhId to get corresponding nodeId
  public long getNode(SwhId swhId) {
    long start = 0;
    long end = nbIds;

    while (start <= end) {
      long lineNumber = (start + end) / 2L;
      String[] parts = swhToNodeMap.readLine(lineNumber).split(" ");
      String currentSwhId = parts[0];
      long currentNodeId = Long.parseLong(parts[1]);

      int cmp = currentSwhId.compareTo(swhId.toString());
      if (cmp == 0) {
        return currentNodeId;
      } else if (cmp < 0) {
        start = lineNumber + 1;
      } else {
        end = lineNumber - 1;
      }
    }

    return -1;
  }

  // WebGraph node id (long) -> SWH id (string)
  // Each line in .nodeToSwh.csv is formatted as: swhId
  // The file is ordered by nodeId, meaning node0's swhId is at line 0, hence we can read the
  // nodeId-th line to get corresponding swhId
  public SwhId getSwhId(long node) {
    String swhId = nodeToSwhMap.readLine(node);
    return new SwhId(swhId);
  }

  void dump() throws ClassNotFoundException, IOException {
    // First internal mapping: SWH id (string) -> WebGraph MPH (long)
    @SuppressWarnings("unchecked")
    Object2LongFunction<String> mphMap =
      (Object2LongFunction<String>) BinIO.loadObject(graphPath + ".mph");

    // Second internal mapping: WebGraph MPH (long) -> BFS ordering (long)
    long[][] bfsMap = LongBigArrays.newBigArray(nbIds);
    long loaded = BinIO.loadLongs(graphPath + ".order", bfsMap);
    if (loaded != nbIds) {
      throw new IllegalArgumentException("Graph contains " + nbIds + " nodes, but read " + loaded);
    }

    // Dump complete mapping for all nodes: SWH id (string) <=> WebGraph node id (long)
    MMapOutputFile swhToNodeMapOut =
      new MMapOutputFile(graphPath + ".swhToNodeMap.csv", SWH_TO_NODE_LINE_LENGTH, nbIds);
    MMapOutputFile nodeToSwhMapOut =
      new MMapOutputFile(graphPath + ".nodeToSwhMap.csv", NODE_TO_SWH_LINE_LENGTH, nbIds);

    InputStream nodeFile = new GZIPInputStream(new FileInputStream(graphPath + ".nodes.csv.gz"));
    FastBufferedReader fileBuffer = new FastBufferedReader(new InputStreamReader(nodeFile, "UTF-8"));
    LineIterator lineIterator = new LineIterator(fileBuffer);

    for (long iNode = 0; iNode < nbIds && lineIterator.hasNext(); iNode++) {
      String swhId = lineIterator.next().toString();
      long mphId = mphMap.getLong(swhId);
      long nodeId = LongBigArrays.get(bfsMap, mphId);

      {
        String paddedNodeId = String.format("%0" + NODE_ID_SIZE + "d", nodeId);
        String line = swhId + " " + paddedNodeId + "\n";
        long lineIndex = iNode;
        swhToNodeMapOut.writeLine(line, lineIndex);
      }

      {
        String line = swhId + "\n";
        long lineIndex = nodeId;
        nodeToSwhMapOut.writeLine(line, lineIndex);
      }
    }

    swhToNodeMapOut.close();
    nodeToSwhMapOut.close();
  }

  public void close() {
    swhToNodeMap.close();
    nodeToSwhMap.close();
  }
}

package org.softwareheritage.graph.algo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.softwareheritage.graph.Graph;

/*
  TODO:
    - merge the two stats files (.properties and .stats) into one
*/

public class Stats {
  public long nbNodes;
  public long nbEdges;
  public double compressionRatio;
  public double bitsPerNode;
  public double bitsPerEdge;
  public double avgLocality;
  public long minIndegree;
  public long maxIndegree;
  public double avgIndegree;
  public long minOutdegree;
  public long maxOutdegree;
  public double avgOutdegree;

  public Stats(String graphPath) throws IOException {
    HashMap<String, String> statsMap = new HashMap<>();

    // Parse statistics from generated files
    Path dotProperties = Paths.get(graphPath + ".properties");
    Path dotStats = Paths.get(graphPath + ".stats");
    List<String> lines = Files.readAllLines(dotProperties);
    lines.addAll(Files.readAllLines(dotStats));
    for (String line : lines) {
      String[] parts = line.split("=");
      if (parts.length == 2) {
        statsMap.put(parts[0], parts[1]);
      }
    }

    this.nbNodes = Long.parseLong(statsMap.get("nodes"));
    this.nbEdges = Long.parseLong(statsMap.get("arcs"));
    this.compressionRatio = Double.parseDouble(statsMap.get("compratio"));
    this.bitsPerNode = Double.parseDouble(statsMap.get("bitspernode"));
    this.bitsPerEdge = Double.parseDouble(statsMap.get("bitsperlink"));
    this.avgLocality = Double.parseDouble(statsMap.get("avglocality"));
    this.minIndegree = Long.parseLong(statsMap.get("minindegree"));
    this.maxIndegree = Long.parseLong(statsMap.get("maxindegree"));
    this.avgIndegree = Double.parseDouble(statsMap.get("avgindegree"));
    this.minOutdegree = Long.parseLong(statsMap.get("minoutdegree"));
    this.maxOutdegree = Long.parseLong(statsMap.get("maxoutdegree"));
    this.avgOutdegree = Double.parseDouble(statsMap.get("avgoutdegree"));
  }
}

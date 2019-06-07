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
  public class Counts {
    public long nodes;
    public long edges;
  }

  public class Ratios {
    public double compression;
    public double bitsPerNode;
    public double bitsPerEdge;
    public double avgLocality;
  }

  public class Degree {
    public long min;
    public long max;
    public double avg;
  }

  public Counts counts;
  public Ratios ratios;
  public Degree indegree;
  public Degree outdegree;

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

    this.counts = new Counts();
    this.ratios = new Ratios();
    this.indegree = new Degree();
    this.outdegree = new Degree();

    this.counts.nodes = Long.parseLong(statsMap.get("nodes"));
    this.counts.edges = Long.parseLong(statsMap.get("arcs"));
    this.ratios.compression = Double.parseDouble(statsMap.get("compratio"));
    this.ratios.bitsPerNode = Double.parseDouble(statsMap.get("bitspernode"));
    this.ratios.bitsPerEdge = Double.parseDouble(statsMap.get("bitsperlink"));
    this.ratios.avgLocality = Double.parseDouble(statsMap.get("avglocality"));
    this.indegree.min = Long.parseLong(statsMap.get("minindegree"));
    this.indegree.max = Long.parseLong(statsMap.get("maxindegree"));
    this.indegree.avg = Double.parseDouble(statsMap.get("avgindegree"));
    this.outdegree.min = Long.parseLong(statsMap.get("minoutdegree"));
    this.outdegree.max = Long.parseLong(statsMap.get("maxoutdegree"));
    this.outdegree.avg = Double.parseDouble(statsMap.get("avgoutdegree"));
  }
}

package org.softwareheritage.graph.algo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.softwareheritage.graph.Dataset;
import org.softwareheritage.graph.Graph;

public class Stats
{
    class DatasetStat {
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
    }

    Graph graph;

    public Stats(Graph graph)
    {
        this.graph = graph;
    }

    public DatasetStat getStats(Dataset.Name datasetName) throws IOException
    {
        Dataset dataset = graph.getDataset(datasetName);
        HashMap<String, String> statsMap = new HashMap<>();

        // Parse statistics from generated files
        Path dotProperties = Paths.get(dataset.getPath() + ".properties");
        Path dotStats = Paths.get(dataset.getPath() + ".stats");
        List<String> lines = Files.readAllLines(dotProperties);
        lines.addAll(Files.readAllLines(dotStats));
        for (String line : lines) {
            String[] parts = line.split("=");
            if (parts.length == 2)
                statsMap.put(parts[0], parts[1]);
        }

        DatasetStat stats = new DatasetStat();
        stats.nbNodes = Long.parseLong(statsMap.get("nodes"));
        stats.nbEdges = Long.parseLong(statsMap.get("arcs"));
        stats.compressionRatio = Double.parseDouble(statsMap.get("compratio"));
        stats.bitsPerNode = Double.parseDouble(statsMap.get("bitspernode"));
        stats.bitsPerEdge = Double.parseDouble(statsMap.get("bitsperlink"));
        stats.avgLocality = Double.parseDouble(statsMap.get("avglocality"));
        stats.minIndegree = Long.parseLong(statsMap.get("minindegree"));
        stats.maxIndegree = Long.parseLong(statsMap.get("maxindegree"));
        stats.avgIndegree = Double.parseDouble(statsMap.get("avgindegree"));
        stats.minOutdegree = Long.parseLong(statsMap.get("minoutdegree"));
        stats.maxOutdegree = Long.parseLong(statsMap.get("maxoutdegree"));
        stats.avgOutdegree = Double.parseDouble(statsMap.get("avgoutdegree"));

        return stats;
    }
}

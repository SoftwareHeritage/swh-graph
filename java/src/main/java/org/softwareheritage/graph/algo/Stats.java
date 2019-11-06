package org.softwareheritage.graph.algo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Statistics on the compressed graph.
 * <p>
 * These statistics are not computed but directly read from <a
 * href="http://webgraph.di.unimi.it/">WebGraph</a> generated .stats and .properties files.
 *
 * @author The Software Heritage developers
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

    /**
     * Constructor.
     *
     * @param graphPath path and basename of compressed graph
     */
    public Stats(String graphPath) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(graphPath + ".properties"));
        properties.load(new FileInputStream(graphPath + ".stats"));

        this.counts = new Counts();
        this.ratios = new Ratios();
        this.indegree = new Degree();
        this.outdegree = new Degree();

        this.counts.nodes = Long.parseLong(properties.getProperty("nodes"));
        this.counts.edges = Long.parseLong(properties.getProperty("arcs"));
        this.ratios.compression = Double.parseDouble(properties.getProperty("compratio"));
        this.ratios.bitsPerNode = Double.parseDouble(properties.getProperty("bitspernode"));
        this.ratios.bitsPerEdge = Double.parseDouble(properties.getProperty("bitsperlink"));
        this.ratios.avgLocality = Double.parseDouble(properties.getProperty("avglocality"));
        this.indegree.min = Long.parseLong(properties.getProperty("minindegree"));
        this.indegree.max = Long.parseLong(properties.getProperty("maxindegree"));
        this.indegree.avg = Double.parseDouble(properties.getProperty("avgindegree"));
        this.outdegree.min = Long.parseLong(properties.getProperty("minoutdegree"));
        this.outdegree.max = Long.parseLong(properties.getProperty("maxoutdegree"));
        this.outdegree.avg = Double.parseDouble(properties.getProperty("avgoutdegree"));
    }
}

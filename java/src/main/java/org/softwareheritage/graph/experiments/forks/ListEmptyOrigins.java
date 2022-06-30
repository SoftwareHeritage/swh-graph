/*
 * Copyright (c) 2019 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.experiments.forks;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.SwhType;

import java.io.IOException;
import java.util.ArrayList;

public class ListEmptyOrigins {
    private SwhBidirectionalGraph graph;
    private Long emptySnapshot;

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ListEmptyOrigins.class.getName(), "",
                    new Parameter[]{new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            'g', "graph", "Basename of the compressed graph"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    public static void main(String[] args) {
        JSAPResult config = parse_args(args);
        String graphPath = config.getString("graphPath");

        ListEmptyOrigins leo = new ListEmptyOrigins();
        try {
            leo.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }
        ArrayList<Long> badlist = leo.compute(leo.graph);
        for (Long bad : badlist) {
            System.out.println(bad);
        }
    }

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = SwhBidirectionalGraph.loadMapped(graphBasename);
        System.err.println("Graph loaded.");
        this.emptySnapshot = null;
    }

    private boolean nodeIsEmptySnapshot(Long node) {
        System.err.println(this.graph.getNodeType(node) + " " + this.graph.outdegree(node) + " " + node);
        if (this.emptySnapshot == null && this.graph.getNodeType(node) == SwhType.SNP
                && this.graph.outdegree(node) == 0) {
            System.err.println("Found empty snapshot: " + node);
            this.emptySnapshot = node;
        }
        return node.equals(this.emptySnapshot);
    }

    private ArrayList<Long> compute(ImmutableGraph graph) {
        final long n = graph.numNodes();
        ArrayList<Long> bad = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            SwhType nt = this.graph.getNodeType(i);
            if (nt != SwhType.ORI)
                continue;

            final LazyLongIterator iterator = graph.successors(i);
            long succ;
            boolean found = false;
            while ((succ = iterator.nextLong()) != -1) {
                if (this.graph.outdegree(succ) > 0) {
                    found = true;
                }
            }
            if (!found)
                bad.add(i);
        }
        return bad;
    }
}

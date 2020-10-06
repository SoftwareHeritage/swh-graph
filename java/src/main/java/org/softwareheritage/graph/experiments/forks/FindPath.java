package org.softwareheritage.graph.experiments.forks;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;

import java.io.IOException;
import java.util.*;

public class FindPath {
    private Graph graph;
    private Long emptySnapshot;

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename).symmetrize();
        System.err.println("Graph loaded.");
        this.emptySnapshot = null;
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(FindPath.class.getName(), "",
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

    private boolean nodeIsEmptySnapshot(Long node) {
        if (this.emptySnapshot == null && this.graph.getNodeType(node) == Node.Type.SNP
                && this.graph.outdegree(node) == 0) {
            System.err.println("Found empty snapshot: " + node);
            this.emptySnapshot = node;
        }
        return node.equals(this.emptySnapshot);
    }

    private Boolean shouldVisit(Long node) {
        Node.Type nt = this.graph.getNodeType(node);
        if (nt != Node.Type.REV && nt != Node.Type.REL && nt != Node.Type.SNP && nt != Node.Type.ORI) {
            return false;
        }
        if (this.nodeIsEmptySnapshot(node))
            return false;
        return true;
    }

    private ArrayList<Long> findPath(Long src, Long dst) {
        HashSet<Long> visited = new HashSet<>();
        Queue<Long> queue = new ArrayDeque<>();
        Map<Long, Long> parentNode = new HashMap<>();

        queue.add(src);
        visited.add(src);

        while (!queue.isEmpty()) {
            long currentNode = queue.poll();

            final LazyLongIterator iterator = graph.successors(currentNode);
            long succ;
            while ((succ = iterator.nextLong()) != -1) {
                if (!shouldVisit(succ) || visited.contains(succ))
                    continue;
                visited.add(succ);
                queue.add(succ);
                parentNode.put(succ, currentNode);

                if (succ == dst) {
                    ArrayList<Long> path = new ArrayList<>();
                    long n = dst;
                    while (n != src) {
                        path.add(n);
                        n = parentNode.get(n);
                    }
                    path.add(src);
                    Collections.reverse(path);
                    return path;
                }
            }
        }
        return null;
    }

    public static void main(String[] args) {
        JSAPResult config = parse_args(args);

        String graphPath = config.getString("graphPath");

        FindPath fpath = new FindPath();
        try {
            fpath.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        Scanner input = new Scanner(System.in);
        while (input.hasNextLong()) {
            long lhsNode = input.nextLong();
            long rhsNode = input.nextLong();

            ArrayList<Long> path = fpath.findPath(lhsNode, rhsNode);
            if (path != null) {
                for (Long n : path) {
                    System.out.format("%d ", n);
                }
                System.out.println();
            } else {
                System.out.println("null");
            }
        }
    }
}

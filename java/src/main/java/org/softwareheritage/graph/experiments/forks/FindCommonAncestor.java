package org.softwareheritage.graph.experiments.forks;

import com.martiansoftware.jsap.*;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Traversal;

import java.io.IOException;
import java.util.Scanner;

public class FindCommonAncestor {
    private Graph graph;

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename);
        System.err.println("Graph loaded.");
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(FindCommonAncestor.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("edgesFmt", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'e',
                                    "edges", "Edges constraints"),
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),});

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
        String edgesFmt = config.getString("edgesFmt");

        FindCommonAncestor fca = new FindCommonAncestor();
        try {
            fca.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        Scanner input = new Scanner(System.in);
        while (input.hasNextLong()) {
            long lhsNode = input.nextLong();
            long rhsNode = input.nextLong();

            Traversal t = new Traversal(fca.graph.symmetrize(), "forward", edgesFmt);
            System.out.println(t.findCommonDescendant(lhsNode, rhsNode));
        }
    }
}

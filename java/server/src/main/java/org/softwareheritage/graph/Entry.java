package org.softwareheritage.graph;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import py4j.GatewayServer;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.algo.NodeIdsConsumer;
import org.softwareheritage.graph.algo.Traversal;

public class Entry {
    Graph graph;

    public void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename);
        System.err.println("Graph loaded.");
    }

    public void visit(long srcNodeId, String direction, String edgesFmt,
                      String clientFIFO) {
        Traversal t = new Traversal(this.graph, direction, edgesFmt);
        try {
            FileOutputStream file = new FileOutputStream(clientFIFO);
            DataOutputStream data = new DataOutputStream(file);
            t.visitNodesVisitor(srcNodeId, (nodeId) -> {
                    try {
                        data.writeLong(nodeId);
                    } catch (IOException e) {
                        throw new RuntimeException("cannot write response to client: " + e);
                    }});
            data.close();
        } catch (IOException e) {
            System.err.println("cannot write response to client: " + e);
        }
    }
}

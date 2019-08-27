package org.softwareheritage.graph;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import py4j.GatewayServer;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.algo.NodeIdsConsumer;
import org.softwareheritage.graph.algo.Traversal;

public class Py4JEntryPoint {

    static final int GATEWAY_SERVER_PORT = 25333;

    Graph graph;

    public Py4JEntryPoint(String graphBasename) throws IOException {
	System.out.println("loading graph " + graphBasename + " ...");
	this.graph = new Graph(graphBasename);
	System.out.println("graph loaded.");
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

    public static void main(String[] args) {
	if (args.length != 1) {
	    System.out.println("Usage: Py4JEntryPoint GRAPH_BASENAME");
	    System.exit(1);
	}

	GatewayServer server = null;
	try {
	    server = new GatewayServer(new Py4JEntryPoint(args[0]), GATEWAY_SERVER_PORT);
	} catch (IOException e) {
	    System.out.println("Could not load graph: " + e);
	    System.exit(2);
	}
        server.start();
        System.out.println("swh-graph: Py4J gateway server started");
    }

}

package org.softwareheritage.graph;

import java.util.Map;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import py4j.GatewayServer;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.algo.Stats;
import org.softwareheritage.graph.algo.NodeIdsConsumer;
import org.softwareheritage.graph.algo.Traversal;

public class Entry {
    Graph graph;

    public void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename);
        System.err.println("Graph loaded.");
    }

    public String stats() {
        try {
            Stats stats = new Stats(graph.getPath());
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
            String res = objectMapper.writeValueAsString(stats);
            return res;
        } catch (IOException e) {
            throw new RuntimeException("Cannot read stats: " + e);
        }
    }

    public QueryHandler get_handler(String clientFIFO) {
        return new QueryHandler(this.graph.copy(), clientFIFO);
    }

    public class QueryHandler {
        Graph graph;
        DataOutputStream out;
        String clientFIFO;

        public QueryHandler(Graph graph, String clientFIFO) {
            this.graph = graph;
            this.clientFIFO = clientFIFO;
            this.out = null;
        }

        public void writeNode(long nodeId) {
            try {
                out.writeLong(nodeId);
            } catch (IOException e) {
                throw new RuntimeException("Cannot write response to client: " + e);
            }
        }

        public void open() {
            try {
                FileOutputStream file = new FileOutputStream(this.clientFIFO);
                this.out = new DataOutputStream(file);
            } catch (IOException e) {
                throw new RuntimeException("Cannot create FIFO: " + e);
            }
        }

        public void close() {
            try {
                out.close();
            } catch (IOException e) {
                throw new RuntimeException("Cannot write response to client: " + e);
            }
        }

        public void leaves(String direction, String edgesFmt, long srcNodeId) {
            open();
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            t.leavesVisitor(srcNodeId, this::writeNode);
            close();
        }

        public void neighbors(String direction, String edgesFmt, long srcNodeId) {
            open();
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            t.neighborsVisitor(srcNodeId, this::writeNode);
            close();
        }

        public void visit_nodes(String direction, String edgesFmt, long srcNodeId) {
            open();
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            t.visitNodesVisitor(srcNodeId, this::writeNode);
            close();
        }

        public void walk(String direction, String edgesFmt, String algorithm,
                             long srcNodeId, long dstNodeId) {
            open();
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            for (Long nodeId : t.walk(srcNodeId, dstNodeId, algorithm)) {
                writeNode(nodeId);
            }
            close();
        }

        public void walk_type(String direction, String edgesFmt, String algorithm,
                              long srcNodeId, String dst) {
            open();
            Node.Type dstType = Node.Type.fromStr(dst);
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            for (Long nodeId : t.walk(srcNodeId, dstType, algorithm)) {
                writeNode(nodeId);
            }
            close();
        }
    }
}

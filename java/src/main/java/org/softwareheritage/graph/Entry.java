package org.softwareheritage.graph;

import java.util.ArrayList;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.softwareheritage.graph.algo.NodeIdConsumer;

import org.softwareheritage.graph.algo.Stats;
import org.softwareheritage.graph.algo.Traversal;

public class Entry {
    private Graph graph;

    private final long PATH_SEPARATOR_ID = -1;

    public void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename);
        System.err.println("Graph loaded.");
    }

    public Graph get_graph() {
        return graph.copy();
    }

    public String stats() {
        try {
            Stats stats = new Stats(graph.getPath());
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
            return objectMapper.writeValueAsString(stats);
        } catch (IOException e) {
            throw new RuntimeException("Cannot read stats: " + e);
        }
    }

    private interface NodeCountVisitor {
        void accept(long nodeId, NodeIdConsumer consumer);
    }

    private int count_visitor(NodeCountVisitor f, long srcNodeId) {
        int count[] = { 0 };
        f.accept(srcNodeId, (node) -> { count[0]++; });
        return count[0];
    }

    public int count_leaves(String direction, String edgesFmt, long srcNodeId) {
        Traversal t = new Traversal(this.graph.copy(), direction, edgesFmt);
        return count_visitor(t::leavesVisitor, srcNodeId);
    }

    public int count_neighbors(String direction, String edgesFmt, long srcNodeId) {
        Traversal t = new Traversal(this.graph.copy(), direction, edgesFmt);
        return count_visitor(t::neighborsVisitor, srcNodeId);
    }

    public int count_visit_nodes(String direction, String edgesFmt, long srcNodeId) {
        Traversal t = new Traversal(this.graph.copy(), direction, edgesFmt);
        return count_visitor(t::visitNodesVisitor, srcNodeId);
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

        public void writeEdge(long srcId, long dstId) {
            writeNode(srcId);
            writeNode(dstId);
        }

        public void writePath(ArrayList<Long> path) {
            for (Long nodeId : path) {
                writeNode(nodeId);
            }
            writeNode(PATH_SEPARATOR_ID);
        }

        public void open() {
            try {
                FileOutputStream file = new FileOutputStream(this.clientFIFO);
                this.out = new DataOutputStream(file);
            } catch (IOException e) {
                throw new RuntimeException("Cannot open client FIFO: " + e);
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

        public void visit_edges(String direction, String edgesFmt, long srcNodeId) {
            open();
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            t.visitNodesVisitor(srcNodeId, null, this::writeEdge);
            close();
        }

        public void visit_paths(String direction, String edgesFmt,
                                long srcNodeId) {
            open();
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            t.visitPathsVisitor(srcNodeId, this::writePath);
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

        public void random_walk(String direction, String edgesFmt, int retries,
                                long srcNodeId, long dstNodeId) {
            open();
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            for (Long nodeId : t.randomWalk(srcNodeId, dstNodeId, retries)) {
                writeNode(nodeId);
            }
            close();
        }

        public void random_walk_type(String direction, String edgesFmt, int retries,
                                     long srcNodeId, String dst) {
            open();
            Node.Type dstType = Node.Type.fromStr(dst);
            Traversal t = new Traversal(this.graph, direction, edgesFmt);
            for (Long nodeId : t.randomWalk(srcNodeId, dstType, retries)) {
                writeNode(nodeId);
            }
            close();
        }
    }
}

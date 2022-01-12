package org.softwareheritage.graph;

import java.io.*;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

public class Entry {
    private Graph graph;

    public void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = Graph.loadMapped(graphBasename);
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

    public void check_swhid(String src) {
        graph.getNodeId(new SWHID(src));
    }

    private int count_visitor(NodeCountVisitor f, long srcNodeId) {
        int[] count = {0};
        f.accept(srcNodeId, (node) -> {
            count[0]++;
        });
        return count[0];
    }

    public int count_leaves(String direction, String edgesFmt, String src, long maxEdges) {
        long srcNodeId = graph.getNodeId(new SWHID(src));
        Traversal t = new Traversal(graph.copy(), direction, edgesFmt, maxEdges);
        return count_visitor(t::leavesVisitor, srcNodeId);
    }

    public int count_neighbors(String direction, String edgesFmt, String src, long maxEdges) {
        long srcNodeId = graph.getNodeId(new SWHID(src));
        Traversal t = new Traversal(graph.copy(), direction, edgesFmt, maxEdges);
        return count_visitor(t::neighborsVisitor, srcNodeId);
    }

    public int count_visit_nodes(String direction, String edgesFmt, String src, long maxEdges) {
        long srcNodeId = graph.getNodeId(new SWHID(src));
        Traversal t = new Traversal(graph.copy(), direction, edgesFmt, maxEdges);
        return count_visitor(t::visitNodesVisitor, srcNodeId);
    }

    public QueryHandler get_handler(String clientFIFO) {
        return new QueryHandler(graph.copy(), clientFIFO);
    }

    private interface NodeCountVisitor {
        void accept(long nodeId, Traversal.NodeIdConsumer consumer);
    }

    public class QueryHandler {
        Graph graph;
        BufferedWriter out;
        String clientFIFO;

        public QueryHandler(Graph graph, String clientFIFO) {
            this.graph = graph;
            this.clientFIFO = clientFIFO;
            this.out = null;
        }

        public void writeNode(SWHID swhid) {
            try {
                out.write(swhid.toString() + "\n");
            } catch (IOException e) {
                throw new RuntimeException("Cannot write response to client: " + e);
            }
        }

        public void writeEdge(SWHID src, SWHID dst) {
            try {
                out.write(src.toString() + " " + dst.toString() + "\n");
            } catch (IOException e) {
                throw new RuntimeException("Cannot write response to client: " + e);
            }
        }

        public void open() {
            try {
                FileOutputStream file = new FileOutputStream(this.clientFIFO);
                this.out = new BufferedWriter(new OutputStreamWriter(file));
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

        public void leaves(String direction, String edgesFmt, String src, long maxEdges, String returnTypes) {
            long srcNodeId = graph.getNodeId(new SWHID(src));
            open();
            Traversal t = new Traversal(graph, direction, edgesFmt, maxEdges, returnTypes);
            for (Long nodeId : t.leaves(srcNodeId)) {
                writeNode(graph.getSWHID(nodeId));
            }
            close();
        }

        public void neighbors(String direction, String edgesFmt, String src, long maxEdges, String returnTypes) {
            long srcNodeId = graph.getNodeId(new SWHID(src));
            open();
            Traversal t = new Traversal(graph, direction, edgesFmt, maxEdges, returnTypes);
            for (Long nodeId : t.neighbors(srcNodeId)) {
                writeNode(graph.getSWHID(nodeId));
            }
            close();
        }

        public void visit_nodes(String direction, String edgesFmt, String src, long maxEdges, String returnTypes) {
            long srcNodeId = graph.getNodeId(new SWHID(src));
            open();
            Traversal t = new Traversal(graph, direction, edgesFmt, maxEdges, returnTypes);
            for (Long nodeId : t.visitNodes(srcNodeId)) {
                writeNode(graph.getSWHID(nodeId));
            }
            close();
        }

        public void visit_edges(String direction, String edgesFmt, String src, long maxEdges, String returnTypes) {
            long srcNodeId = graph.getNodeId(new SWHID(src));
            open();
            Traversal t = new Traversal(graph, direction, edgesFmt, maxEdges);
            t.visitNodesVisitor(srcNodeId, null, (srcId, dstId) -> {
                writeEdge(graph.getSWHID(srcId), graph.getSWHID(dstId));
            });
            close();
        }

        public void walk(String direction, String edgesFmt, String algorithm, String src, String dst, long maxEdges,
                String returnTypes) {
            long srcNodeId = graph.getNodeId(new SWHID(src));
            open();
            ArrayList<Long> res;
            Traversal t = new Traversal(graph, direction, edgesFmt, maxEdges, returnTypes);
            if (dst.matches("ori|snp|rel|rev|dir|cnt")) {
                Node.Type dstType = Node.Type.fromStr(dst);
                res = t.walk(srcNodeId, dstType, algorithm);
            } else {
                long dstNodeId = graph.getNodeId(new SWHID(dst));
                res = t.walk(srcNodeId, dstNodeId, algorithm);
            }
            for (Long nodeId : res) {
                writeNode(graph.getSWHID(nodeId));
            }
            close();
        }

        public void random_walk(String direction, String edgesFmt, int retries, String src, String dst, long maxEdges,
                String returnTypes) {
            long srcNodeId = graph.getNodeId(new SWHID(src));
            open();
            ArrayList<Long> res;
            Traversal t = new Traversal(graph, direction, edgesFmt, maxEdges, returnTypes);
            if (dst.matches("ori|snp|rel|rev|dir|cnt")) {
                Node.Type dstType = Node.Type.fromStr(dst);
                res = t.randomWalk(srcNodeId, dstType, retries);
            } else {
                long dstNodeId = graph.getNodeId(new SWHID(dst));
                res = t.randomWalk(srcNodeId, dstNodeId, retries);
            }
            for (Long nodeId : res) {
                writeNode(graph.getSWHID(nodeId));
            }
            close();
        }
    }
}

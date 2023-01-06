package org.softwareheritage.graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Stack;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.softwareheritage.graph.server.Endpoint;

import it.unimi.dsi.big.webgraph.LazyLongIterator;

/**
 * Traversal algorithms on the compressed graph.
 * <p>
 * Internal implementation of the traversal API endpoints. These methods only input/output internal
 * long ids, which are converted in the {@link Endpoint} higher-level class to {@link SWHID}.
 *
 * @author The Software Heritage developers
 * @see Endpoint
 */

public class Traversal {
    /** Graph used in the traversal */
    Graph graph;
    /** Graph edge restriction */
    AllowedEdges edges;

    /** Hash set storing if we have visited a node */
    HashSet<Long> visited;
    /** Hash map storing parent node id for each nodes during a traversal */
    Map<Long, Long> parentNode;
    /** Number of edges accessed during traversal */
    long nbEdgesAccessed;

    /** The anti Dos limit of edges traversed while a visit */
    long maxEdges;
    /** The string represent the set of type restriction */
    NodesFiltering ndsfilter;

    long currentEdgeAccessed = 0;

    /** random number generator, for random walks */
    Random rng;

    /**
     * Constructor.
     *
     * @param graph graph used in the traversal
     * @param direction a string (either "forward" or "backward") specifying edge orientation
     * @param edgesFmt a formatted string describing <a href=
     *            "https://docs.softwareheritage.org/devel/swh-graph/api.html#terminology">allowed
     *            edges</a>
     */

    public Traversal(Graph graph, String direction, String edgesFmt) {
        this(graph, direction, edgesFmt, 0);
    }

    public Traversal(Graph graph, String direction, String edgesFmt, long maxEdges) {
        this(graph, direction, edgesFmt, maxEdges, "*");
    }

    public Traversal(Graph graph, String direction, String edgesFmt, long maxEdges, String returnTypes) {
        if (!direction.matches("forward|backward")) {
            throw new IllegalArgumentException("Unknown traversal direction: " + direction);
        }

        if (direction.equals("backward")) {
            this.graph = graph.transpose();
        } else {
            this.graph = graph;
        }
        this.edges = new AllowedEdges(edgesFmt);

        this.visited = new HashSet<>();
        this.parentNode = new HashMap<>();
        this.nbEdgesAccessed = 0;
        this.maxEdges = maxEdges;
        this.rng = new Random();

        if (returnTypes.equals("*")) {
            this.ndsfilter = new NodesFiltering();
        } else {
            this.ndsfilter = new NodesFiltering(returnTypes);
        }
    }

    /**
     * Returns number of accessed edges during traversal.
     *
     * @return number of edges accessed in last traversal
     */
    public long getNbEdgesAccessed() {
        return nbEdgesAccessed;
    }

    /**
     * Returns number of accessed nodes during traversal.
     *
     * @return number of nodes accessed in last traversal
     */
    public long getNbNodesAccessed() {
        return this.visited.size();
    }

    /**
     * Push version of {@link #leaves} will fire passed callback for each leaf.
     */
    public void leavesVisitor(long srcNodeId, NodeIdConsumer cb) {
        Stack<Long> stack = new Stack<>();
        this.nbEdgesAccessed = 0;
        stack.push(srcNodeId);
        visited.add(srcNodeId);

        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();

            long neighborsCnt = 0;
            nbEdgesAccessed += graph.outdegree(currentNodeId);

            LazyLongIterator it = graph.successors(currentNodeId, edges);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                neighborsCnt++;
                if (this.maxEdges > 0) {
                    if (neighborsCnt >= this.maxEdges) {
                        return;
                    }
                }
                if (!visited.contains(neighborNodeId)) {
                    stack.push(neighborNodeId);
                    visited.add(neighborNodeId);
                }
            }

            if (neighborsCnt == 0) {
                cb.accept(currentNodeId);
            }
        }
    }

    /**
     * Returns the leaves of a subgraph rooted at the specified source node.
     *
     * @param srcNodeId source node
     * @return list of node ids corresponding to the leaves
     */
    public ArrayList<Long> leaves(long srcNodeId) {
        ArrayList<Long> nodeIds = new ArrayList<Long>();
        leavesVisitor(srcNodeId, nodeIds::add);
        if (ndsfilter.restricted) {
            return ndsfilter.filterByNodeTypes(nodeIds, graph);
        }
        return nodeIds;
    }

    /**
     * Push version of {@link #neighbors}: will fire passed callback on each neighbor.
     */
    public void neighborsVisitor(long srcNodeId, NodeIdConsumer cb) {
        this.nbEdgesAccessed = graph.outdegree(srcNodeId);
        LazyLongIterator it = graph.successors(srcNodeId, edges);
        for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
            cb.accept(neighborNodeId);
            this.currentEdgeAccessed++;
            if (this.maxEdges > 0) {
                if (this.currentEdgeAccessed == this.maxEdges) {
                    return;
                }
            }
        }
    }

    /**
     * Returns node direct neighbors (linked with exactly one edge).
     *
     * @param srcNodeId source node
     * @return list of node ids corresponding to the neighbors
     */
    public ArrayList<Long> neighbors(long srcNodeId) {
        ArrayList<Long> nodeIds = new ArrayList<>();
        neighborsVisitor(srcNodeId, nodeIds::add);
        if (ndsfilter.restricted) {
            return ndsfilter.filterByNodeTypes(nodeIds, graph);
        }
        return nodeIds;
    }

    /**
     * Push version of {@link #visitNodes}: will fire passed callback on each visited node.
     */
    public void visitNodesVisitor(long srcNodeId, NodeIdConsumer nodeCb, EdgeIdConsumer edgeCb) {
        Stack<Long> stack = new Stack<>();
        this.nbEdgesAccessed = 0;
        stack.push(srcNodeId);
        visited.add(srcNodeId);

        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();
            if (nodeCb != null) {
                if (this.maxEdges > 0) {
                    // we can go through n arcs, so at the end we must have
                    // the source node + n nodes reached through these n arcs
                    if (this.currentEdgeAccessed > this.maxEdges) {
                        break;
                    }
                }
                nodeCb.accept(currentNodeId);
                this.currentEdgeAccessed++;
            }
            nbEdgesAccessed += graph.outdegree(currentNodeId);
            LazyLongIterator it = graph.successors(currentNodeId, edges);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                if (edgeCb != null) {
                    if (this.maxEdges > 0) {
                        if (this.currentEdgeAccessed == this.maxEdges) {
                            return;
                        }
                    }
                    edgeCb.accept(currentNodeId, neighborNodeId);
                    this.currentEdgeAccessed++;

                }
                if (!visited.contains(neighborNodeId)) {
                    stack.push(neighborNodeId);
                    visited.add(neighborNodeId);
                }
            }
        }
    }

    /** One-argument version to handle callbacks properly */
    public void visitNodesVisitor(long srcNodeId, NodeIdConsumer cb) {
        visitNodesVisitor(srcNodeId, cb, null);
    }

    /**
     * Performs a graph traversal and returns explored nodes.
     *
     * @param srcNodeId source node
     * @return list of explored node ids
     */
    public ArrayList<Long> visitNodes(long srcNodeId) {
        ArrayList<Long> nodeIds = new ArrayList<>();
        visitNodesVisitor(srcNodeId, nodeIds::add);
        if (ndsfilter.restricted) {
            return ndsfilter.filterByNodeTypes(nodeIds, graph);
        }
        return nodeIds;
    }

    /**
     * Push version of {@link #visitPaths}: will fire passed callback on each discovered (complete)
     * path.
     */
    public void visitPathsVisitor(long srcNodeId, PathConsumer cb) {
        Stack<Long> currentPath = new Stack<>();
        this.nbEdgesAccessed = 0;
        visitPathsInternalVisitor(srcNodeId, currentPath, cb);
    }

    /**
     * Performs a graph traversal and returns explored paths.
     *
     * @param srcNodeId source node
     * @return list of explored paths (represented as a list of node ids)
     */
    public ArrayList<ArrayList<Long>> visitPaths(long srcNodeId) {
        ArrayList<ArrayList<Long>> paths = new ArrayList<>();
        visitPathsVisitor(srcNodeId, paths::add);
        return paths;
    }

    private void visitPathsInternalVisitor(long currentNodeId, Stack<Long> currentPath, PathConsumer cb) {
        currentPath.push(currentNodeId);
        long visitedNeighbors = 0;
        nbEdgesAccessed += graph.outdegree(currentNodeId);
        LazyLongIterator it = graph.successors(currentNodeId, edges);
        for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
            if (this.maxEdges > 0) {
                if (this.currentEdgeAccessed == this.maxEdges) {
                    break;
                }
            }
            this.currentEdgeAccessed++;
            visitPathsInternalVisitor(neighborNodeId, currentPath, cb);
            visitedNeighbors++;
        }

        if (visitedNeighbors == 0) {
            ArrayList<Long> path = new ArrayList<>(currentPath);
            cb.accept(path);
        }

        currentPath.pop();
    }

    /**
     * Performs a graph traversal with backtracking, and returns the first found path from source to
     * destination.
     *
     * @param srcNodeId source node
     * @param dst destination (either a node or a node type)
     * @return found path as a list of node ids
     */
    public <T> ArrayList<Long> walk(long srcNodeId, T dst, String visitOrder) {
        long dstNodeId;
        if (visitOrder.equals("dfs")) {
            dstNodeId = walkInternalDFS(srcNodeId, dst);
        } else if (visitOrder.equals("bfs")) {
            dstNodeId = walkInternalBFS(srcNodeId, dst);
        } else {
            throw new IllegalArgumentException("Unknown visit order: " + visitOrder);
        }

        if (dstNodeId == -1) {
            throw new IllegalArgumentException("Cannot find destination: " + dst);
        }

        return backtracking(srcNodeId, dstNodeId);
    }

    /**
     * Performs a random walk (picking a random successor at each step) from source to destination.
     *
     * @param srcNodeId source node
     * @param dst destination (either a node or a node type)
     * @return found path as a list of node ids or an empty path to indicate that no suitable path have
     *         been found
     */
    public <T> ArrayList<Long> randomWalk(long srcNodeId, T dst) {
        return randomWalk(srcNodeId, dst, 0);
    }

    /**
     * Performs a stubborn random walk (picking a random successor at each step) from source to
     * destination. The walk is "stubborn" in the sense that it will not give up the first time if a
     * satisfying target node is found, but it will retry up to a limited amount of times.
     *
     * @param srcNodeId source node
     * @param dst destination (either a node or a node type)
     * @param retries number of times to retry; 0 means no retries (single walk)
     * @return found path as a list of node ids or an empty path to indicate that no suitable path have
     *         been found
     */
    public <T> ArrayList<Long> randomWalk(long srcNodeId, T dst, int retries) {
        long curNodeId = srcNodeId;
        ArrayList<Long> path = new ArrayList<>();
        this.nbEdgesAccessed = 0;
        boolean found;

        if (retries < 0) {
            throw new IllegalArgumentException("Negative number of retries given: " + retries);
        }

        while (true) {
            path.add(curNodeId);
            LazyLongIterator successors = graph.successors(curNodeId, edges);
            curNodeId = randomPick(successors);
            if (curNodeId < 0) {
                found = false;
                break;
            }
            if (isDstNode(curNodeId, dst)) {
                path.add(curNodeId);
                found = true;
                break;
            }
        }

        if (found) {
            if (ndsfilter.restricted) {
                return ndsfilter.filterByNodeTypes(path, graph);
            }
            return path;
        } else if (retries > 0) { // try again
            return randomWalk(srcNodeId, dst, retries - 1);
        } else { // not found and no retries left
            path.clear();
            return path;
        }
    }

    /**
     * Randomly choose an element from an iterator over Longs using reservoir sampling
     *
     * @param elements iterator over selection domain
     * @return randomly chosen element or -1 if no suitable element was found
     */
    private long randomPick(LazyLongIterator elements) {
        long curPick = -1;
        long seenCandidates = 0;

        for (long element; (element = elements.nextLong()) != -1;) {
            seenCandidates++;
            if (Math.round(rng.nextFloat() * (seenCandidates - 1)) == 0) {
                curPick = element;
            }
        }

        return curPick;
    }

    /**
     * Internal DFS function of {@link #walk}.
     *
     * @param srcNodeId source node
     * @param dst destination (either a node or a node type)
     * @return final destination node or -1 if no path found
     */
    private <T> long walkInternalDFS(long srcNodeId, T dst) {
        Stack<Long> stack = new Stack<>();
        this.nbEdgesAccessed = 0;

        stack.push(srcNodeId);
        visited.add(srcNodeId);

        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();
            if (isDstNode(currentNodeId, dst)) {
                return currentNodeId;
            }

            nbEdgesAccessed += graph.outdegree(currentNodeId);
            LazyLongIterator it = graph.successors(currentNodeId, edges);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                if (!visited.contains(neighborNodeId)) {
                    stack.push(neighborNodeId);
                    visited.add(neighborNodeId);
                    parentNode.put(neighborNodeId, currentNodeId);
                }
            }
        }

        return -1;
    }

    /**
     * Internal BFS function of {@link #walk}.
     *
     * @param srcNodeId source node
     * @param dst destination (either a node or a node type)
     * @return final destination node or -1 if no path found
     */
    private <T> long walkInternalBFS(long srcNodeId, T dst) {
        Queue<Long> queue = new LinkedList<>();
        this.nbEdgesAccessed = 0;

        queue.add(srcNodeId);
        visited.add(srcNodeId);

        while (!queue.isEmpty()) {
            long currentNodeId = queue.poll();
            if (isDstNode(currentNodeId, dst)) {
                return currentNodeId;
            }

            nbEdgesAccessed += graph.outdegree(currentNodeId);
            LazyLongIterator it = graph.successors(currentNodeId, edges);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                if (!visited.contains(neighborNodeId)) {
                    queue.add(neighborNodeId);
                    visited.add(neighborNodeId);
                    parentNode.put(neighborNodeId, currentNodeId);
                }
            }
        }

        return -1;
    }

    /**
     * Internal function of {@link #walk} to check if a node corresponds to the destination.
     *
     * @param nodeId current node
     * @param dst destination (either a node or a node type)
     * @return true if the node is a destination, or false otherwise
     */
    private <T> boolean isDstNode(long nodeId, T dst) {
        if (dst instanceof Long) {
            long dstNodeId = (Long) dst;
            return nodeId == dstNodeId;
        } else if (dst instanceof Node.Type) {
            Node.Type dstType = (Node.Type) dst;
            return graph.getNodeType(nodeId) == dstType;
        } else {
            return false;
        }
    }

    /**
     * Internal backtracking function of {@link #walk}.
     *
     * @param srcNodeId source node
     * @param dstNodeId destination node
     * @return the found path, as a list of node ids
     */
    private ArrayList<Long> backtracking(long srcNodeId, long dstNodeId) {
        ArrayList<Long> path = new ArrayList<>();
        long currentNodeId = dstNodeId;
        while (currentNodeId != srcNodeId) {
            path.add(currentNodeId);
            currentNodeId = parentNode.get(currentNodeId);
        }
        path.add(srcNodeId);
        Collections.reverse(path);
        return path;
    }

    /**
     * Find a common descendant between two given nodes using two parallel BFS
     *
     * @param lhsNode the first node
     * @param rhsNode the second node
     * @return the found path, as a list of node ids
     */
    public Long findCommonDescendant(long lhsNode, long rhsNode) {
        Queue<Long> lhsStack = new ArrayDeque<>();
        Queue<Long> rhsStack = new ArrayDeque<>();
        HashSet<Long> lhsVisited = new HashSet<>();
        HashSet<Long> rhsVisited = new HashSet<>();
        lhsStack.add(lhsNode);
        rhsStack.add(rhsNode);
        lhsVisited.add(lhsNode);
        rhsVisited.add(rhsNode);

        this.nbEdgesAccessed = 0;
        Long curNode;

        while (!lhsStack.isEmpty() || !rhsStack.isEmpty()) {
            if (!lhsStack.isEmpty()) {
                curNode = lhsStack.poll();
                nbEdgesAccessed += graph.outdegree(curNode);
                LazyLongIterator it = graph.successors(curNode, edges);
                for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                    if (!lhsVisited.contains(neighborNodeId)) {
                        if (rhsVisited.contains(neighborNodeId))
                            return neighborNodeId;
                        lhsStack.add(neighborNodeId);
                        lhsVisited.add(neighborNodeId);
                    }
                }
            }

            if (!rhsStack.isEmpty()) {
                curNode = rhsStack.poll();
                nbEdgesAccessed += graph.outdegree(curNode);
                LazyLongIterator it = graph.successors(curNode, edges);
                for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                    if (!rhsVisited.contains(neighborNodeId)) {
                        if (lhsVisited.contains(neighborNodeId))
                            return neighborNodeId;
                        rhsStack.add(neighborNodeId);
                        rhsVisited.add(neighborNodeId);
                    }
                }
            }
        }

        return null;
    }

    public interface NodeIdConsumer extends LongConsumer {
        /**
         * Callback for incrementally receiving node identifiers during a graph visit.
         */
        void accept(long nodeId);
    }

    public interface EdgeIdConsumer {
        /**
         * Callback for incrementally receiving edge identifiers during a graph visit.
         */
        void accept(long srcId, long dstId);
    }

    public interface PathConsumer extends Consumer<ArrayList<Long>> {
        /**
         * Callback for incrementally receiving node paths (made of node identifiers) during a graph visit.
         */
        void accept(ArrayList<Long> path);
    }
}

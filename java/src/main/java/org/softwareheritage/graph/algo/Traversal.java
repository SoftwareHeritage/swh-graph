package org.softwareheritage.graph.algo;

import java.util.*;

import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Neighbors;
import org.softwareheritage.graph.Node;

/**
 * Traversal algorithms on the compressed graph.
 * <p>
 * Internal implementation of the traversal API endpoints. These methods only input/output internal
 * long ids, which are converted in the {@link Endpoint} higher-level class to Software Heritage
 * PID.
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.Endpoint
 */

public class Traversal {
    /** Graph used in the traversal */
    Graph graph;
    /** Boolean to specify the use of the transposed graph */
    boolean useTransposed;
    /** Graph edge restriction */
    AllowedEdges edges;

    /** Hash set storing if we have visited a node */
    HashSet<Long> visited;
    /** Hash map storing parent node id for each nodes during a traversal */
    Map<Long, Long> parentNode;
    /** Number of edges accessed during traversal */
    long nbEdgesAccessed;

    /** random number generator, for random walks */
    Random rng;

    /**
     * Constructor.
     *
     * @param graph graph used in the traversal
     * @param direction a string (either "forward" or "backward") specifying edge orientation
     * @param edgesFmt a formatted string describing <a
     * href="https://docs.softwareheritage.org/devel/swh-graph/api.html#terminology">allowed edges</a>
     */
    public Traversal(Graph graph, String direction, String edgesFmt) {
        if (!direction.matches("forward|backward")) {
            throw new IllegalArgumentException("Unknown traversal direction: " + direction);
        }

        this.graph = graph;
        this.useTransposed = (direction.equals("backward"));
        this.edges = new AllowedEdges(graph, edgesFmt);

        long nbNodes = graph.getNbNodes();
        this.visited = new HashSet<>();
        this.parentNode = new HashMap<>();
        this.nbEdgesAccessed = 0;
        this.rng = new Random();
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
     * Push version of {@link leaves}: will fire passed callback for each leaf.
     */
    public void leavesVisitor(long srcNodeId, NodeIdConsumer cb) {
        Stack<Long> stack = new Stack<Long>();
        this.nbEdgesAccessed = 0;

        stack.push(srcNodeId);
        visited.add(srcNodeId);

        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();

            long neighborsCnt = 0;
            nbEdgesAccessed += graph.degree(currentNodeId, useTransposed);
            for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
                neighborsCnt++;
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
        leavesVisitor(srcNodeId, (nodeId) -> nodeIds.add(nodeId));
        return nodeIds;
    }

    /**
     * Push version of {@link neighbors}: will fire passed callback on each
     * neighbor.
     */
    public void neighborsVisitor(long srcNodeId, NodeIdConsumer cb) {
        this.nbEdgesAccessed = graph.degree(srcNodeId, useTransposed);
        for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, srcNodeId)) {
            cb.accept(neighborNodeId);
        }
    }

    /**
     * Returns node direct neighbors (linked with exactly one edge).
     *
     * @param srcNodeId source node
     * @return list of node ids corresponding to the neighbors
     */
    public ArrayList<Long> neighbors(long srcNodeId) {
        ArrayList<Long> nodeIds = new ArrayList<Long>();
        neighborsVisitor(srcNodeId, (nodeId) -> nodeIds.add(nodeId));
        return nodeIds;
    }

    /**
     * Push version of {@link visitNodes}: will fire passed callback on each
     * visited node.
     */
    public void visitNodesVisitor(long srcNodeId, NodeIdConsumer nodeCb, EdgeIdConsumer edgeCb) {
        Stack<Long> stack = new Stack<Long>();
        this.nbEdgesAccessed = 0;

        stack.push(srcNodeId);
        visited.add(srcNodeId);

        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();
            if (nodeCb != null) {
                nodeCb.accept(currentNodeId);
            }

            nbEdgesAccessed += graph.degree(currentNodeId, useTransposed);
            for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
                if (edgeCb != null) {
                    edgeCb.accept(currentNodeId, neighborNodeId);
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
        ArrayList<Long> nodeIds = new ArrayList<Long>();
        visitNodesVisitor(srcNodeId, (nodeId) -> nodeIds.add(nodeId));
        return nodeIds;
    }

    /**
     * Push version of {@link visitPaths}: will fire passed callback on each
     * discovered (complete) path.
     */
    public void visitPathsVisitor(long srcNodeId, PathConsumer cb) {
        Stack<Long> currentPath = new Stack<Long>();
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
        visitPathsVisitor(srcNodeId, (path) -> paths.add(path));
        return paths;
    }

    private void visitPathsInternalVisitor(long currentNodeId,
                                           Stack<Long> currentPath,
                                           PathConsumer cb) {
        currentPath.push(currentNodeId);

        long visitedNeighbors = 0;
        nbEdgesAccessed += graph.degree(currentNodeId, useTransposed);
        for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
            visitPathsInternalVisitor(neighborNodeId, currentPath, cb);
            visitedNeighbors++;
        }

        if (visitedNeighbors == 0) {
            ArrayList<Long> path = new ArrayList<Long>();
            for (long nodeId : currentPath) {
                path.add(nodeId);
            }
            cb.accept(path);
        }

        currentPath.pop();
    }

    /**
     * Performs a graph traversal with backtracking, and returns the first
     * found path from source to destination.
     *
     * @param srcNodeId source node
     * @param dst destination (either a node or a node type)
     * @return found path as a list of node ids
     */
    public <T> ArrayList<Long> walk(long srcNodeId, T dst, String visitOrder) {
        long dstNodeId = -1;
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

        ArrayList<Long> nodeIds = backtracking(srcNodeId, dstNodeId);
        return nodeIds;
    }

    /**
     * Performs a random walk (picking a random successor at each step) from
     * source to destination.
     *
     * @param srcNodeId source node
     * @param dst destination (either a node or a node type)
     * @return found path as a list of node ids or an empty path to indicate
     * that no suitable path have been found
     */
    public <T> ArrayList<Long> randomWalk(long srcNodeId, T dst) {
        return randomWalk(srcNodeId, dst, 0);
    }

    /**
     * Performs a stubborn random walk (picking a random successor at each
     * step) from source to destination. The walk is "stubborn" in the sense
     * that it will not give up the first time if a satisfying target node is
     * found, but it will retry up to a limited amount of times.
     *
     * @param srcNodeId source node
     * @param dst destination (either a node or a node type)
     * @param retries number of times to retry; 0 means no retries (single walk)
     * @return found path as a list of node ids or an empty path to indicate
     * that no suitable path have been found
     */
    public <T> ArrayList<Long> randomWalk(long srcNodeId, T dst, int retries) {
        long curNodeId = srcNodeId;
        ArrayList<Long> path = new ArrayList<Long>();
        this.nbEdgesAccessed = 0;
        boolean found;

        if (retries < 0) {
            throw new IllegalArgumentException("Negative number of retries given: " + retries);
        }

        while (true) {
            path.add(curNodeId);
            Neighbors neighbors = new Neighbors(graph, useTransposed, edges, curNodeId);
            curNodeId = randomPick(neighbors.iterator());
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
            return path;
        } else if (retries > 0) {  // try again
            return randomWalk(srcNodeId, dst, retries - 1);
        } else {  // not found and no retries left
            path.clear();
            return path;
        }
    }

    /**
     * Randomly choose an element from an iterator over Longs using reservoir
     * sampling
     *
     * @param elements iterator over selection domain
     * @return randomly chosen element or -1 if no suitable element was found
     */
    private long randomPick(Iterator<Long> elements) {
        long curPick = -1;
        long seenCandidates = 0;

        while (elements.hasNext()) {
            seenCandidates++;
            if (Math.round(rng.nextFloat() * (seenCandidates - 1)) == 0) {
                curPick = elements.next();
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
        Stack<Long> stack = new Stack<Long>();
        this.nbEdgesAccessed = 0;

        stack.push(srcNodeId);
        visited.add(srcNodeId);

        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();
            if (isDstNode(currentNodeId, dst)) {
                return currentNodeId;
            }

            nbEdgesAccessed += graph.degree(currentNodeId, useTransposed);
            for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
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
        Queue<Long> queue = new LinkedList<Long>();
        this.nbEdgesAccessed = 0;

        queue.add(srcNodeId);
        visited.add(srcNodeId);

        while (!queue.isEmpty()) {
            long currentNodeId = queue.poll();
            if (isDstNode(currentNodeId, dst)) {
                return currentNodeId;
            }

            nbEdgesAccessed += graph.degree(currentNodeId, useTransposed);
            for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
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
        ArrayList<Long> path = new ArrayList<Long>();
        long currentNodeId = dstNodeId;
        while (currentNodeId != srcNodeId) {
            path.add(currentNodeId);
            currentNodeId = parentNode.get(currentNodeId);
        }
        path.add(srcNodeId);
        Collections.reverse(path);
        return path;
    }

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
                nbEdgesAccessed += graph.degree(curNode, useTransposed);
                for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, curNode)) {
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
                nbEdgesAccessed += graph.degree(curNode, useTransposed);
                for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, curNode)) {
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
}

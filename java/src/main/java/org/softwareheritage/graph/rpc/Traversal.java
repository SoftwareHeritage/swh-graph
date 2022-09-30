/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.Label;
import org.softwareheritage.graph.*;

import java.util.*;

/** Traversal contains all the algorithms used for graph traversals */
public class Traversal {
    /**
     * Wrapper around g.successors(), only follows edges that are allowed by the given
     * {@link AllowedEdges} object.
     */
    private static ArcLabelledNodeIterator.LabelledArcIterator filterLabelledSuccessors(SwhUnidirectionalGraph g,
            long nodeId, AllowedEdges allowedEdges) {
        if (allowedEdges.restrictedTo == null) {
            // All edges are allowed, bypass edge check
            return g.labelledSuccessors(nodeId);
        } else {
            ArcLabelledNodeIterator.LabelledArcIterator allSuccessors = g.labelledSuccessors(nodeId);
            return new ArcLabelledNodeIterator.LabelledArcIterator() {
                @Override
                public Label label() {
                    return allSuccessors.label();
                }

                @Override
                public long nextLong() {
                    long neighbor;
                    while ((neighbor = allSuccessors.nextLong()) != -1) {
                        if (allowedEdges.isAllowed(g.getNodeType(nodeId), g.getNodeType(neighbor))) {
                            return neighbor;
                        }
                    }
                    return -1;
                }

                @Override
                public long skip(final long n) {
                    long i = 0;
                    while (i < n && nextLong() != -1)
                        i++;
                    return i;
                }
            };
        }
    }

    /** Helper class to check that a given node is "valid" for some given {@link NodeFilter} */
    private static class NodeFilterChecker {
        private final SwhUnidirectionalGraph g;
        private final NodeFilter filter;
        private final AllowedNodes allowedNodes;

        private NodeFilterChecker(SwhUnidirectionalGraph graph, NodeFilter filter) {
            this.g = graph;
            this.filter = filter;
            this.allowedNodes = new AllowedNodes(filter.hasTypes() ? filter.getTypes() : "*");
        }

        public boolean allowed(long nodeId) {
            if (filter == null) {
                return true;
            }
            if (!this.allowedNodes.isAllowed(g.getNodeType(nodeId))) {
                return false;
            }

            return true;
        }
    }

    /** Returns the unidirectional graph from a bidirectional graph and a {@link GraphDirection}. */
    public static SwhUnidirectionalGraph getDirectedGraph(SwhBidirectionalGraph g, GraphDirection direction) {
        switch (direction) {
            case FORWARD:
                return g.getForwardGraph();
            case BACKWARD:
                return g.getBackwardGraph();
            /*
             * TODO: add support for BOTH case BOTH: return new SwhUnidirectionalGraph(g.symmetrize(),
             * g.getProperties());
             */
            default :
                throw new IllegalArgumentException("Unknown direction: " + direction);
        }
    }

    /** Returns the opposite of a given {@link GraphDirection} (equivalent to a graph transposition). */
    public static GraphDirection reverseDirection(GraphDirection direction) {
        switch (direction) {
            case FORWARD:
                return GraphDirection.BACKWARD;
            case BACKWARD:
                return GraphDirection.FORWARD;
            /*
             * TODO: add support for BOTH case BOTH: return GraphDirection.BOTH;
             */
            default :
                throw new IllegalArgumentException("Unknown direction: " + direction);
        }
    }

    /** Dummy exception to short-circuit and interrupt a graph traversal. */
    static class StopTraversalException extends RuntimeException {
    }

    /** Generic BFS traversal algorithm. */
    static class BFSVisitor {
        /** The graph to traverse. */
        protected final SwhUnidirectionalGraph g;
        /** Depth of the node currently being visited */
        protected long depth = 0;
        /**
         * Number of traversal successors (i.e., successors that will be considered by the traversal) of the
         * node currently being visited
         */
        protected long traversalSuccessors = 0;
        /** Number of edges accessed since the beginning of the traversal */
        protected long edgesAccessed = 0;

        /**
         * Map from a node ID to its parent node ID. The key set can be used as the set of all visited
         * nodes.
         */
        protected HashMap<Long, Long> parents = new HashMap<>();
        /** Queue of nodes to visit (also called "frontier", "open set", "wavefront" etc.) */
        protected ArrayDeque<Long> queue = new ArrayDeque<>();
        /** If > 0, the maximum depth of the traversal. */
        private long maxDepth = -1;
        /** If > 0, the maximum number of edges to traverse. */
        private long maxEdges = -1;

        BFSVisitor(SwhUnidirectionalGraph g) {
            this.g = g;
        }

        /** Add a new source node to the initial queue. */
        public void addSource(long nodeId) {
            queue.add(nodeId);
            parents.put(nodeId, -1L);
        }

        /** Set the maximum depth of the traversal. */
        public void setMaxDepth(long depth) {
            maxDepth = depth;
        }

        /** Set the maximum number of edges to traverse. */
        public void setMaxEdges(long edges) {
            maxEdges = edges;
        }

        /** Setup the visit counters and depth sentinel. */
        public void visitSetup() {
            edgesAccessed = 0;
            depth = 0;
            queue.add(-1L); // depth sentinel
        }

        /** Perform the visit */
        public void visit() {
            visitSetup();
            while (!queue.isEmpty()) {
                visitStep();
            }
        }

        /** Single "step" of a visit. Advance the frontier of exactly one node. */
        public void visitStep() {
            try {
                assert !queue.isEmpty();
                long curr = queue.poll();
                if (curr == -1L) {
                    ++depth;
                    if (!queue.isEmpty()) {
                        queue.add(-1L);
                        visitStep();
                    }
                    return;
                }
                if (maxDepth >= 0 && depth > maxDepth) {
                    throw new StopTraversalException();
                }
                edgesAccessed += g.outdegree(curr);
                if (maxEdges >= 0 && edgesAccessed > maxEdges) {
                    throw new StopTraversalException();
                }
                visitNode(curr);
            } catch (StopTraversalException e) {
                // Traversal is over, clear the to-do queue.
                queue.clear();
            }
        }

        /**
         * Get the successors of a node. Override this function if you want to filter which successors are
         * considered during the traversal.
         */
        protected ArcLabelledNodeIterator.LabelledArcIterator getSuccessors(long nodeId) {
            return g.labelledSuccessors(nodeId);
        }

        /** Visit a node. Override to do additional processing on the node. */
        protected void visitNode(long node) {
            ArcLabelledNodeIterator.LabelledArcIterator it = getSuccessors(node);
            traversalSuccessors = 0;
            for (long succ; (succ = it.nextLong()) != -1;) {
                traversalSuccessors++;
                visitEdge(node, succ, it.label());
            }
        }

        /** Visit an edge. Override to do additional processing on the edge. */
        protected void visitEdge(long src, long dst, Label label) {
            if (!parents.containsKey(dst)) {
                queue.add(dst);
                parents.put(dst, src);
            }
        }
    }

    /**
     * SimpleTraversal is used by the Traverse endpoint. It extends BFSVisitor with additional
     * processing, notably related to graph properties and filters.
     */
    static class SimpleTraversal extends BFSVisitor {
        private final NodeFilterChecker nodeReturnChecker;
        private final AllowedEdges allowedEdges;
        private final TraversalRequest request;
        private final NodePropertyBuilder.NodeDataMask nodeDataMask;
        private final NodeObserver nodeObserver;
        private long remainingMatches;

        private Node.Builder nodeBuilder;

        SimpleTraversal(SwhBidirectionalGraph bidirectionalGraph, TraversalRequest request, NodeObserver nodeObserver) {
            super(getDirectedGraph(bidirectionalGraph, request.getDirection()));
            this.request = request;
            this.nodeObserver = nodeObserver;
            this.nodeReturnChecker = new NodeFilterChecker(g, request.getReturnNodes());
            this.nodeDataMask = new NodePropertyBuilder.NodeDataMask(request.hasMask() ? request.getMask() : null);
            this.allowedEdges = new AllowedEdges(request.hasEdges() ? request.getEdges() : "*");
            request.getSrcList().forEach(srcSwhid -> {
                long srcNodeId = g.getNodeId(new SWHID(srcSwhid));
                addSource(srcNodeId);
            });
            if (request.hasMaxDepth()) {
                setMaxDepth(request.getMaxDepth());
            }
            if (request.hasMaxEdges()) {
                setMaxEdges(request.getMaxEdges());
            }
            if (request.hasMaxMatchingNodes() && request.getMaxMatchingNodes() > 0) {
                this.remainingMatches = request.getMaxMatchingNodes();
            } else {
                this.remainingMatches = -1;
            }
        }

        @Override
        protected ArcLabelledNodeIterator.LabelledArcIterator getSuccessors(long nodeId) {
            return filterLabelledSuccessors(g, nodeId, allowedEdges);
        }

        @Override
        public void visitNode(long node) {
            nodeBuilder = null;
            if (nodeReturnChecker.allowed(node) && (!request.hasMinDepth() || depth >= request.getMinDepth())) {
                nodeBuilder = Node.newBuilder();
                NodePropertyBuilder.buildNodeProperties(g, nodeDataMask, nodeBuilder, node);
            }
            super.visitNode(node);

            if (request.getReturnNodes().hasMinTraversalSuccessors()
                    && traversalSuccessors < request.getReturnNodes().getMinTraversalSuccessors()) {
                nodeBuilder = null;
            }
            if (request.getReturnNodes().hasMaxTraversalSuccessors()
                    && traversalSuccessors > request.getReturnNodes().getMaxTraversalSuccessors()) {
                nodeBuilder = null;
            }

            if (nodeBuilder != null) {
                nodeObserver.onNext(nodeBuilder.build());

                if (remainingMatches >= 0) {
                    remainingMatches--;
                    if (remainingMatches == 0) {
                        // We matched as many nodes as allowed
                        throw new StopTraversalException();
                    }
                }
            }
        }

        @Override
        protected void visitEdge(long src, long dst, Label label) {
            super.visitEdge(src, dst, label);
            NodePropertyBuilder.buildSuccessorProperties(g, nodeDataMask, nodeBuilder, src, dst, label);
        }
    }

    /**
     * FindPathTo searches for a path from a source node to a node matching a given criteria It extends
     * BFSVisitor with additional processing, and makes the traversal stop as soon as a node matching
     * the given criteria is found.
     */
    static class FindPathTo extends BFSVisitor {
        private final AllowedEdges allowedEdges;
        private final FindPathToRequest request;
        private final NodePropertyBuilder.NodeDataMask nodeDataMask;
        private final NodeFilterChecker targetChecker;
        private Long targetNode = null;

        FindPathTo(SwhBidirectionalGraph bidirectionalGraph, FindPathToRequest request) {
            super(getDirectedGraph(bidirectionalGraph, request.getDirection()));
            this.request = request;
            this.targetChecker = new NodeFilterChecker(g, request.getTarget());
            this.nodeDataMask = new NodePropertyBuilder.NodeDataMask(request.hasMask() ? request.getMask() : null);
            this.allowedEdges = new AllowedEdges(request.hasEdges() ? request.getEdges() : "*");
            if (request.hasMaxDepth()) {
                setMaxDepth(request.getMaxDepth());
            }
            if (request.hasMaxEdges()) {
                setMaxEdges(request.getMaxEdges());
            }
            request.getSrcList().forEach(srcSwhid -> {
                long srcNodeId = g.getNodeId(new SWHID(srcSwhid));
                addSource(srcNodeId);
            });
        }

        @Override
        protected ArcLabelledNodeIterator.LabelledArcIterator getSuccessors(long nodeId) {
            return filterLabelledSuccessors(g, nodeId, allowedEdges);
        }

        @Override
        public void visitNode(long node) {
            if (targetChecker.allowed(node)) {
                targetNode = node;
                throw new StopTraversalException();
            }
            super.visitNode(node);
        }

        /**
         * Once the visit has been performed and a matching node has been found, return the shortest path
         * from the source set to that node. To do so, we need to backtrack the parents of the node until we
         * find one of the source nodes (whose parent is -1).
         */
        public Path getPath() {
            if (targetNode == null) {
                return null; // No path found.
            }

            /* Backtrack from targetNode to a source node */
            long curNode = targetNode;
            ArrayList<Long> path = new ArrayList<>();
            while (curNode != -1) {
                path.add(curNode);
                curNode = parents.get(curNode);
            }
            Collections.reverse(path);

            /* Enrich path with node properties */
            Path.Builder pathBuilder = Path.newBuilder();
            for (long nodeId : path) {
                Node.Builder nodeBuilder = Node.newBuilder();
                NodePropertyBuilder.buildNodeProperties(g, nodeDataMask, nodeBuilder, nodeId);
                pathBuilder.addNode(nodeBuilder.build());
            }
            return pathBuilder.build();
        }
    }

    /**
     * FindPathBetween searches for a shortest path between a set of source nodes and a set of
     * destination nodes.
     *
     * It does so by performing a *bidirectional breadth-first search*, i.e., two parallel breadth-first
     * searches, one from the source set ("src-BFS") and one from the destination set ("dst-BFS"), until
     * both searches find a common node that joins their visited sets. This node is called the "midpoint
     * node". The path returned is the path src -> ... -> midpoint -> ... -> dst, which is always a
     * shortest path between src and dst.
     *
     * The graph direction of both BFS can be configured separately. By default, the dst-BFS will use
     * the graph in the opposite direction than the src-BFS (if direction = FORWARD, by default
     * direction_reverse = BACKWARD, and vice-versa). The default behavior is thus to search for a
     * shortest path between two nodes in a given direction. However, one can also specify FORWARD or
     * BACKWARD for *both* the src-BFS and the dst-BFS. This will search for a common descendant or a
     * common ancestor between the two sets, respectively. These will be the midpoints of the returned
     * path.
     */
    static class FindPathBetween extends BFSVisitor {
        private final FindPathBetweenRequest request;
        private final NodePropertyBuilder.NodeDataMask nodeDataMask;
        private final AllowedEdges allowedEdgesSrc;
        private final AllowedEdges allowedEdgesDst;

        private final BFSVisitor srcVisitor;
        private final BFSVisitor dstVisitor;
        private Long middleNode = null;

        FindPathBetween(SwhBidirectionalGraph bidirectionalGraph, FindPathBetweenRequest request) {
            super(getDirectedGraph(bidirectionalGraph, request.getDirection()));
            this.request = request;
            this.nodeDataMask = new NodePropertyBuilder.NodeDataMask(request.hasMask() ? request.getMask() : null);

            GraphDirection direction = request.getDirection();
            // if direction_reverse is not specified, use the opposite direction of direction
            GraphDirection directionReverse = request.hasDirectionReverse()
                    ? request.getDirectionReverse()
                    : reverseDirection(request.getDirection());
            SwhUnidirectionalGraph srcGraph = getDirectedGraph(bidirectionalGraph, direction);
            SwhUnidirectionalGraph dstGraph = getDirectedGraph(bidirectionalGraph, directionReverse);

            this.allowedEdgesSrc = new AllowedEdges(request.hasEdges() ? request.getEdges() : "*");
            /*
             * If edges_reverse is not specified: - If `edges` is not specified either, defaults to "*" - If
             * direction == direction_reverse, defaults to `edges` - If direction != direction_reverse, defaults
             * to the reverse of `edges` (e.g. "rev:dir" becomes "dir:rev").
             */
            this.allowedEdgesDst = request.hasEdgesReverse()
                    ? new AllowedEdges(request.getEdgesReverse())
                    : (request.hasEdges()
                            ? (direction == directionReverse
                                    ? new AllowedEdges(request.getEdges())
                                    : new AllowedEdges(request.getEdges()).reverse())
                            : new AllowedEdges("*"));

            /*
             * Source sub-visitor. Aborts as soon as it finds a node already visited by the destination
             * sub-visitor.
             */
            this.srcVisitor = new BFSVisitor(srcGraph) {
                @Override
                protected ArcLabelledNodeIterator.LabelledArcIterator getSuccessors(long nodeId) {
                    return filterLabelledSuccessors(g, nodeId, allowedEdgesSrc);
                }

                @Override
                public void visitNode(long node) {
                    if (dstVisitor.parents.containsKey(node)) {
                        middleNode = node;
                        throw new StopTraversalException();
                    }
                    super.visitNode(node);
                }
            };

            /*
             * Destination sub-visitor. Aborts as soon as it finds a node already visited by the source
             * sub-visitor.
             */
            this.dstVisitor = new BFSVisitor(dstGraph) {
                @Override
                protected ArcLabelledNodeIterator.LabelledArcIterator getSuccessors(long nodeId) {
                    return filterLabelledSuccessors(g, nodeId, allowedEdgesDst);
                }

                @Override
                public void visitNode(long node) {
                    if (srcVisitor.parents.containsKey(node)) {
                        middleNode = node;
                        throw new StopTraversalException();
                    }
                    super.visitNode(node);
                }
            };
            if (request.hasMaxDepth()) {
                this.srcVisitor.setMaxDepth(request.getMaxDepth());
                this.dstVisitor.setMaxDepth(request.getMaxDepth());
            }
            if (request.hasMaxEdges()) {
                this.srcVisitor.setMaxEdges(request.getMaxEdges());
                this.dstVisitor.setMaxEdges(request.getMaxEdges());
            }
            request.getSrcList().forEach(srcSwhid -> {
                long srcNodeId = g.getNodeId(new SWHID(srcSwhid));
                srcVisitor.addSource(srcNodeId);
            });
            request.getDstList().forEach(srcSwhid -> {
                long srcNodeId = g.getNodeId(new SWHID(srcSwhid));
                dstVisitor.addSource(srcNodeId);
            });
        }

        @Override
        public void visit() {
            /*
             * Bidirectional BFS: maintain two sub-visitors, and alternately run a visit step in each of them.
             */
            srcVisitor.visitSetup();
            dstVisitor.visitSetup();
            while (!srcVisitor.queue.isEmpty() || !dstVisitor.queue.isEmpty()) {
                if (!srcVisitor.queue.isEmpty()) {
                    srcVisitor.visitStep();
                }
                if (!dstVisitor.queue.isEmpty()) {
                    dstVisitor.visitStep();
                }
            }
        }

        public Path getPath() {
            if (middleNode == null) {
                return null; // No path found.
            }
            Path.Builder pathBuilder = Path.newBuilder();
            ArrayList<Long> path = new ArrayList<>();

            /* First section of the path: src -> midpoint */
            long curNode = middleNode;
            while (curNode != -1) {
                path.add(curNode);
                curNode = srcVisitor.parents.get(curNode);
            }
            pathBuilder.setMidpointIndex(path.size() - 1);
            Collections.reverse(path);

            /* Second section of the path: midpoint -> dst */
            curNode = dstVisitor.parents.get(middleNode);
            while (curNode != -1) {
                path.add(curNode);
                curNode = dstVisitor.parents.get(curNode);
            }

            /* Enrich path with node properties */
            for (long nodeId : path) {
                Node.Builder nodeBuilder = Node.newBuilder();
                NodePropertyBuilder.buildNodeProperties(g, nodeDataMask, nodeBuilder, nodeId);
                pathBuilder.addNode(nodeBuilder.build());
            }
            return pathBuilder.build();
        }
    }

    public interface NodeObserver {
        void onNext(Node nodeId);
    }
}

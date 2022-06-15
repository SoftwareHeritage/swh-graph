package org.softwareheritage.graph.rpc;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.Label;
import org.softwareheritage.graph.*;

import java.util.*;

public class Traversal {
    private static LazyLongIterator filterSuccessors(SwhUnidirectionalGraph g, long nodeId, AllowedEdges allowedEdges) {
        if (allowedEdges.restrictedTo == null) {
            // All edges are allowed, bypass edge check
            return g.successors(nodeId);
        } else {
            LazyLongIterator allSuccessors = g.successors(nodeId);
            return new LazyLongIterator() {
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

    public static SwhUnidirectionalGraph getDirectedGraph(SwhBidirectionalGraph g, TraversalRequest request) {
        switch (request.getDirection()) {
            case FORWARD:
                return g.getForwardGraph();
            case BACKWARD:
                return g.getBackwardGraph();
            case BOTH:
                return new SwhUnidirectionalGraph(g.symmetrize(), g.getProperties());
        }
        throw new IllegalArgumentException("Unknown direction: " + request.getDirection());
    }

    public static void simpleTraversal(SwhBidirectionalGraph bidirectionalGraph, TraversalRequest request,
            NodeObserver nodeObserver) {
        SwhUnidirectionalGraph g = getDirectedGraph(bidirectionalGraph, request);
        NodeFilterChecker nodeReturnChecker = new NodeFilterChecker(g, request.getReturnNodes());
        NodePropertyBuilder.NodeDataMask nodeDataMask = new NodePropertyBuilder.NodeDataMask(
                request.hasMask() ? request.getMask() : null);

        AllowedEdges allowedEdges = new AllowedEdges(request.hasEdges() ? request.getEdges() : "*");

        Queue<Long> queue = new ArrayDeque<>();
        HashSet<Long> visited = new HashSet<>();
        request.getSrcList().forEach(srcSwhid -> {
            long srcNodeId = g.getNodeId(new SWHID(srcSwhid));
            queue.add(srcNodeId);
            visited.add(srcNodeId);
        });
        queue.add(-1L); // Depth sentinel

        long edgesAccessed = 0;
        long currentDepth = 0;
        while (!queue.isEmpty()) {
            long curr = queue.poll();
            if (curr == -1L) {
                ++currentDepth;
                if (!queue.isEmpty()) {
                    queue.add(-1L);
                }
                continue;
            }
            if (request.hasMaxDepth() && currentDepth > request.getMaxDepth()) {
                break;
            }
            edgesAccessed += g.outdegree(curr);
            if (request.hasMaxEdges() && edgesAccessed >= request.getMaxEdges()) {
                break;
            }

            Node.Builder nodeBuilder = null;
            if (nodeReturnChecker.allowed(curr) && (!request.hasMinDepth() || currentDepth >= request.getMinDepth())) {
                nodeBuilder = Node.newBuilder();
                NodePropertyBuilder.buildNodeProperties(g, nodeDataMask, nodeBuilder, curr);
            }

            ArcLabelledNodeIterator.LabelledArcIterator it = filterLabelledSuccessors(g, curr, allowedEdges);
            long traversalSuccessors = 0;
            for (long succ; (succ = it.nextLong()) != -1;) {
                traversalSuccessors++;
                if (!visited.contains(succ)) {
                    queue.add(succ);
                    visited.add(succ);
                }
                NodePropertyBuilder.buildSuccessorProperties(g, nodeDataMask, nodeBuilder, curr, succ, it.label());
            }
            if (request.getReturnNodes().hasMinTraversalSuccessors()
                    && traversalSuccessors < request.getReturnNodes().getMinTraversalSuccessors()
                    || request.getReturnNodes().hasMaxTraversalSuccessors()
                            && traversalSuccessors > request.getReturnNodes().getMaxTraversalSuccessors()) {
                nodeBuilder = null;
            }
            if (nodeBuilder != null) {
                nodeObserver.onNext(nodeBuilder.build());
            }
        }
    }

    public interface NodeObserver {
        void onNext(Node nodeId);
    }
}

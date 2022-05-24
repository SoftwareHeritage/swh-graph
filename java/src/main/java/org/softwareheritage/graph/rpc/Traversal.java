package org.softwareheritage.graph.rpc;

import com.google.protobuf.ByteString;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.Label;
import org.softwareheritage.graph.*;
import org.softwareheritage.graph.labels.DirEntry;

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
                buildNodeProperties(g, request.getReturnFields(), nodeBuilder, curr);
            }

            ArcLabelledNodeIterator.LabelledArcIterator it = filterLabelledSuccessors(g, curr, allowedEdges);
            long traversalSuccessors = 0;
            for (long succ; (succ = it.nextLong()) != -1;) {
                traversalSuccessors++;
                if (!visited.contains(succ)) {
                    queue.add(succ);
                    visited.add(succ);
                }
                buildSuccessorProperties(g, request.getReturnFields(), nodeBuilder, curr, succ, it.label());
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

    private static void buildNodeProperties(SwhUnidirectionalGraph graph, NodeFields fields, Node.Builder nodeBuilder,
            long node) {
        if (fields == null || !fields.hasSwhid() || fields.getSwhid()) {
            nodeBuilder.setSwhid(graph.getSWHID(node).toString());
        }
        if (fields == null) {
            return;
        }

        switch (graph.getNodeType(node)) {
            case CNT:
                if (fields.hasCntLength()) {
                    nodeBuilder.setCntLength(graph.getContentLength(node));
                }
                if (fields.hasCntIsSkipped()) {
                    nodeBuilder.setCntIsSkipped(graph.isContentSkipped(node));
                }
                break;
            case REV:
                if (fields.getRevAuthor()) {
                    nodeBuilder.setRevAuthor(graph.getAuthorId(node));
                }
                if (fields.getRevCommitter()) {
                    nodeBuilder.setRevAuthor(graph.getCommitterId(node));
                }
                if (fields.getRevAuthorDate()) {
                    nodeBuilder.setRevAuthorDate(graph.getAuthorTimestamp(node));
                }
                if (fields.getRevAuthorDateOffset()) {
                    nodeBuilder.setRevAuthorDateOffset(graph.getAuthorTimestampOffset(node));
                }
                if (fields.getRevCommitterDate()) {
                    nodeBuilder.setRevCommitterDate(graph.getCommitterTimestamp(node));
                }
                if (fields.getRevCommitterDateOffset()) {
                    nodeBuilder.setRevCommitterDateOffset(graph.getCommitterTimestampOffset(node));
                }
                if (fields.getRevMessage()) {
                    byte[] msg = graph.getMessage(node);
                    if (msg != null) {
                        nodeBuilder.setRevMessage(ByteString.copyFrom(msg));
                    }
                }
                break;
            case REL:
                if (fields.getRelAuthor()) {
                    nodeBuilder.setRelAuthor(graph.getAuthorId(node));
                }
                if (fields.getRelAuthorDate()) {
                    nodeBuilder.setRelAuthorDate(graph.getAuthorTimestamp(node));
                }
                if (fields.getRelAuthorDateOffset()) {
                    nodeBuilder.setRelAuthorDateOffset(graph.getAuthorTimestampOffset(node));
                }
                if (fields.getRelName()) {
                    byte[] msg = graph.getTagName(node);
                    if (msg != null) {
                        nodeBuilder.setRelName(ByteString.copyFrom(msg));
                    }
                }
                if (fields.getRelMessage()) {
                    byte[] msg = graph.getMessage(node);
                    if (msg != null) {
                        nodeBuilder.setRelMessage(ByteString.copyFrom(msg));
                    }
                }
                break;
            case ORI:
                if (fields.getOriUrl()) {
                    String url = graph.getUrl(node);
                    if (url != null) {
                        nodeBuilder.setOriUrl(url);
                    }
                }
        }
    }

    private static void buildSuccessorProperties(SwhUnidirectionalGraph graph, NodeFields fields,
            Node.Builder nodeBuilder, long src, long dst, Label label) {
        if (nodeBuilder != null && fields != null && fields.getSuccessor()) {
            Successor.Builder successorBuilder = Successor.newBuilder();
            if (!fields.hasSuccessorSwhid() || fields.getSuccessorSwhid()) {
                successorBuilder.setSwhid(graph.getSWHID(dst).toString());
            }
            if (fields.getSuccessorLabel()) {
                DirEntry[] entries = (DirEntry[]) label.get();
                for (DirEntry entry : entries) {
                    EdgeLabel.Builder builder = EdgeLabel.newBuilder();
                    builder.setName(ByteString.copyFrom(graph.getLabelName(entry.filenameId)));
                    builder.setPermission(entry.permission);
                    successorBuilder.addLabel(builder.build());
                }
            }
            nodeBuilder.addSuccessor(successorBuilder.build());
        }
    }

    public interface NodeObserver {
        void onNext(Node nodeId);
    }
}

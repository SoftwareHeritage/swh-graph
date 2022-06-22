package org.softwareheritage.graph.rpc;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.Label;
import org.softwareheritage.graph.*;

import java.util.*;

public class Traversal {
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

    static class StopTraversalException extends RuntimeException {
    }

    static class BFSVisitor {
        protected final SwhUnidirectionalGraph g;
        protected long depth = 0;
        protected long traversalSuccessors = 0;
        protected long edgesAccessed = 0;

        protected HashMap<Long, Long> parents = new HashMap<>();
        protected ArrayDeque<Long> queue = new ArrayDeque<>();
        private long maxDepth = -1;
        private long maxEdges = -1;

        BFSVisitor(SwhUnidirectionalGraph g) {
            this.g = g;
        }

        public void addSource(long nodeId) {
            queue.add(nodeId);
            parents.put(nodeId, -1L);
        }

        public void setMaxDepth(long depth) {
            maxDepth = depth;
        }

        public void setMaxEdges(long edges) {
            maxEdges = edges;
        }

        public void visitSetup() {
            edgesAccessed = 0;
            depth = 0;
            queue.add(-1L); // depth sentinel
        }

        public void visit() {
            visitSetup();
            try {
                while (!queue.isEmpty()) {
                    visitStep();
                }
            } catch (StopTraversalException e) {
                // Ignore
            }
        }

        public void visitStep() {
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
            if (maxEdges >= 0 && edgesAccessed >= maxEdges) {
                throw new StopTraversalException();
            }
            visitNode(curr);
        }

        protected ArcLabelledNodeIterator.LabelledArcIterator getSuccessors(long nodeId) {
            return g.labelledSuccessors(nodeId);
        }

        protected void visitNode(long node) {
            ArcLabelledNodeIterator.LabelledArcIterator it = getSuccessors(node);
            traversalSuccessors = 0;
            for (long succ; (succ = it.nextLong()) != -1;) {
                traversalSuccessors++;
                visitEdge(node, succ, it.label());
            }
        }

        protected void visitEdge(long src, long dst, Label label) {
            if (!parents.containsKey(dst)) {
                queue.add(dst);
                parents.put(dst, src);
            }
        }
    }

    static class SimpleTraversal extends BFSVisitor {
        private final NodeFilterChecker nodeReturnChecker;
        private final AllowedEdges allowedEdges;
        private final TraversalRequest request;
        private final NodePropertyBuilder.NodeDataMask nodeDataMask;
        private final NodeObserver nodeObserver;

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
                    && traversalSuccessors < request.getReturnNodes().getMinTraversalSuccessors()
                    || request.getReturnNodes().hasMaxTraversalSuccessors()
                            && traversalSuccessors > request.getReturnNodes().getMaxTraversalSuccessors()) {
                nodeBuilder = null;
            }
            if (nodeBuilder != null) {
                nodeObserver.onNext(nodeBuilder.build());
            }
        }

        @Override
        protected void visitEdge(long src, long dst, Label label) {
            super.visitEdge(src, dst, label);
            NodePropertyBuilder.buildSuccessorProperties(g, nodeDataMask, nodeBuilder, src, dst, label);
        }
    }

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

        public Path getPath() {
            if (targetNode == null) {
                return null;
            }
            Path.Builder pathBuilder = Path.newBuilder();
            long curNode = targetNode;
            ArrayList<Long> path = new ArrayList<>();
            while (curNode != -1) {
                path.add(curNode);
                curNode = parents.get(curNode);
            }
            Collections.reverse(path);
            for (long nodeId : path) {
                Node.Builder nodeBuilder = Node.newBuilder();
                NodePropertyBuilder.buildNodeProperties(g, nodeDataMask, nodeBuilder, nodeId);
                pathBuilder.addNode(nodeBuilder.build());
            }
            return pathBuilder.build();
        }
    }

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
            this.allowedEdgesSrc = new AllowedEdges(request.hasEdges() ? request.getEdges() : "*");
            this.allowedEdgesDst = request.hasEdgesReverse()
                    ? new AllowedEdges(request.getEdgesReverse())
                    : (request.hasEdges() ? new AllowedEdges(request.getEdges()).reverse() : new AllowedEdges("*"));
            SwhUnidirectionalGraph srcGraph = getDirectedGraph(bidirectionalGraph, request.getDirection());
            SwhUnidirectionalGraph dstGraph = getDirectedGraph(bidirectionalGraph,
                    request.hasDirectionReverse()
                            ? request.getDirectionReverse()
                            : reverseDirection(request.getDirection()));

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
            this.dstVisitor = new BFSVisitor(dstGraph) {
                @Override
                protected ArcLabelledNodeIterator.LabelledArcIterator getSuccessors(long nodeId) {
                    return filterLabelledSuccessors(g, nodeId, allowedEdgesDst);
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
            srcVisitor.visitSetup();
            dstVisitor.visitSetup();
            while (!srcVisitor.queue.isEmpty() || !dstVisitor.queue.isEmpty()) {
                try {
                    if (!srcVisitor.queue.isEmpty()) {
                        srcVisitor.visitStep();
                    }
                } catch (StopTraversalException e) {
                    srcVisitor.queue.clear();
                }
                try {
                    if (!dstVisitor.queue.isEmpty()) {
                        dstVisitor.visitStep();
                    }
                } catch (StopTraversalException e) {
                    dstVisitor.queue.clear();
                }
            }
        }

        public Path getPath() {
            if (middleNode == null) {
                return null;
            }
            Path.Builder pathBuilder = Path.newBuilder();
            ArrayList<Long> path = new ArrayList<>();
            long curNode = middleNode;
            while (curNode != -1) {
                path.add(curNode);
                curNode = srcVisitor.parents.get(curNode);
            }
            Collections.reverse(path);
            curNode = dstVisitor.parents.get(middleNode);
            while (curNode != -1) {
                path.add(curNode);
                curNode = dstVisitor.parents.get(curNode);
            }
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

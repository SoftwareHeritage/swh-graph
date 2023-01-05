/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.FieldMaskUtil;
import it.unimi.dsi.big.webgraph.labelling.Label;
import org.softwareheritage.graph.SwhUnidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;

import java.util.*;

/**
 * NodePropertyBuilder is a helper class to enrich {@link Node} messages with node and edge
 * properties. It is used by {@link GraphServer.TraversalService} to build the response messages or
 * streams. Because property access is disk-based and slow, particular care is taken to avoid
 * loading unnecessary properties. We use a FieldMask object to check which properties are requested
 * by the client, and only load these.
 */
public class NodePropertyBuilder {
    /**
     * NodeDataMask caches a FieldMask into a more efficient representation (booleans). This avoids the
     * need of parsing the FieldMask for each node in the stream.
     */
    public static class NodeDataMask {
        public boolean swhid;
        public boolean successor;
        public boolean successorSwhid;
        public boolean successorLabel;
        public boolean numSuccessors;
        public boolean cntLength;
        public boolean cntIsSkipped;
        public boolean revAuthor;
        public boolean revAuthorDate;
        public boolean revAuthorDateOffset;
        public boolean revCommitter;
        public boolean revCommitterDate;
        public boolean revCommitterDateOffset;
        public boolean revMessage;
        public boolean relAuthor;
        public boolean relAuthorDate;
        public boolean relAuthorDateOffset;
        public boolean relName;
        public boolean relMessage;
        public boolean oriUrl;

        public NodeDataMask(FieldMask mask) {
            Set<String> allowedFields = null;
            if (mask != null) {
                mask = FieldMaskUtil.normalize(mask);
                allowedFields = new HashSet<>(mask.getPathsList());
            }
            this.swhid = allowedFields == null || allowedFields.contains("swhid");
            this.successorSwhid = allowedFields == null || allowedFields.contains("successor")
                    || allowedFields.contains("successor.swhid");
            this.successorLabel = allowedFields == null || allowedFields.contains("successor")
                    || allowedFields.contains("successor.label");
            this.successor = this.successorSwhid || this.successorLabel;
            this.numSuccessors = allowedFields == null || allowedFields.contains("num_successors");
            this.cntLength = allowedFields == null || allowedFields.contains("cnt.length");
            this.cntIsSkipped = allowedFields == null || allowedFields.contains("cnt.is_skipped");
            this.revAuthor = allowedFields == null || allowedFields.contains("rev.author");
            this.revAuthorDate = allowedFields == null || allowedFields.contains("rev.author_date");
            this.revAuthorDateOffset = allowedFields == null || allowedFields.contains("rev.author_date_offset");
            this.revCommitter = allowedFields == null || allowedFields.contains("rev.committer");
            this.revCommitterDate = allowedFields == null || allowedFields.contains("rev.committer_date");
            this.revCommitterDateOffset = allowedFields == null || allowedFields.contains("rev.committer_date_offset");
            this.revMessage = allowedFields == null || allowedFields.contains("rev.message");
            this.relAuthor = allowedFields == null || allowedFields.contains("rel.author");
            this.relAuthorDate = allowedFields == null || allowedFields.contains("rel.author_date");
            this.relAuthorDateOffset = allowedFields == null || allowedFields.contains("rel.author_date_offset");
            this.relName = allowedFields == null || allowedFields.contains("rel.name");
            this.relMessage = allowedFields == null || allowedFields.contains("rel.message");
            this.oriUrl = allowedFields == null || allowedFields.contains("ori.url");
        }
    }

    /** Enrich a Node message with node properties requested in the NodeDataMask. */
    public static void buildNodeProperties(SwhUnidirectionalGraph graph, NodeDataMask mask, Node.Builder nodeBuilder,
            long node) {
        if (mask.swhid) {
            nodeBuilder.setSwhid(graph.getSWHID(node).toString());
        }

        switch (graph.getNodeType(node)) {
            case CNT:
                ContentData.Builder cntBuilder = ContentData.newBuilder();
                if (mask.cntLength) {
                    cntBuilder.setLength(graph.getContentLength(node));
                }
                if (mask.cntIsSkipped) {
                    cntBuilder.setIsSkipped(graph.isContentSkipped(node));
                }
                nodeBuilder.setCnt(cntBuilder.build());
                break;
            case REV:
                RevisionData.Builder revBuilder = RevisionData.newBuilder();
                if (mask.revAuthor) {
                    revBuilder.setAuthor(graph.getAuthorId(node));
                }
                if (mask.revAuthorDate) {
                    revBuilder.setAuthorDate(graph.getAuthorTimestamp(node));
                }
                if (mask.revAuthorDateOffset) {
                    revBuilder.setAuthorDateOffset(graph.getAuthorTimestampOffset(node));
                }
                if (mask.revCommitter) {
                    revBuilder.setCommitter(graph.getCommitterId(node));
                }
                if (mask.revCommitterDate) {
                    revBuilder.setCommitterDate(graph.getCommitterTimestamp(node));
                }
                if (mask.revCommitterDateOffset) {
                    revBuilder.setCommitterDateOffset(graph.getCommitterTimestampOffset(node));
                }
                if (mask.revMessage) {
                    byte[] msg = graph.getMessage(node);
                    if (msg != null) {
                        revBuilder.setMessage(ByteString.copyFrom(msg));
                    }
                }
                nodeBuilder.setRev(revBuilder.build());
                break;
            case REL:
                ReleaseData.Builder relBuilder = ReleaseData.newBuilder();
                if (mask.relAuthor) {
                    Long author = graph.getAuthorId(node);
                    if (author != null) {
                        relBuilder.setAuthor(author);
                    }
                }
                if (mask.relAuthorDate) {
                    Long date = graph.getAuthorTimestamp(node);
                    if (date != null) {
                        relBuilder.setAuthorDate(date);
                    }
                }
                if (mask.relAuthorDateOffset) {
                    Short offset = graph.getAuthorTimestampOffset(node);
                    if (offset != null) {
                        relBuilder.setAuthorDateOffset(offset);
                    }
                }
                if (mask.relName) {
                    byte[] msg = graph.getMessage(node);
                    if (msg != null) {
                        relBuilder.setMessage(ByteString.copyFrom(msg));
                    }
                }
                if (mask.relMessage) {
                    byte[] msg = graph.getMessage(node);
                    if (msg != null) {
                        relBuilder.setMessage(ByteString.copyFrom(msg));
                    }
                }
                nodeBuilder.setRel(relBuilder.build());
                break;
            case ORI:
                OriginData.Builder oriBuilder = OriginData.newBuilder();
                if (mask.oriUrl) {
                    String url = graph.getUrl(node);
                    if (url != null) {
                        oriBuilder.setUrl(url);
                    }
                }
                nodeBuilder.setOri(oriBuilder.build());
        }
    }

    /** Enrich a Node message with node properties requested in the FieldMask. */
    public static void buildNodeProperties(SwhUnidirectionalGraph graph, FieldMask mask, Node.Builder nodeBuilder,
            long node) {
        NodeDataMask nodeMask = new NodeDataMask(mask);
        buildNodeProperties(graph, nodeMask, nodeBuilder, node);
    }

    /**
     * Enrich a Node message with edge properties requested in the NodeDataMask, for a specific edge.
     */
    public static void buildSuccessorProperties(SwhUnidirectionalGraph graph, NodeDataMask mask,
            Node.Builder nodeBuilder, long src, long dst, Label label) {
        if (nodeBuilder != null) {
            Successor.Builder successorBuilder = Successor.newBuilder();
            if (mask.successorSwhid) {
                successorBuilder.setSwhid(graph.getSWHID(dst).toString());
            }
            if (mask.successorLabel) {
                DirEntry[] entries = (DirEntry[]) label.get();
                for (DirEntry entry : entries) {
                    EdgeLabel.Builder builder = EdgeLabel.newBuilder();
                    builder.setName(ByteString.copyFrom(graph.getLabelName(entry.filenameId)));
                    builder.setPermission(entry.permission);
                    successorBuilder.addLabel(builder.build());
                }
            }
            Successor successor = successorBuilder.build();
            if (successor != Successor.getDefaultInstance()) {
                nodeBuilder.addSuccessor(successor);
            }

            if (mask.numSuccessors) {
                nodeBuilder.setNumSuccessors(nodeBuilder.getNumSuccessors() + 1);
            }
        }
    }

    /** Enrich a Node message with edge properties requested in the FieldMask, for a specific edge. */
    public static void buildSuccessorProperties(SwhUnidirectionalGraph graph, FieldMask mask, Node.Builder nodeBuilder,
            long src, long dst, Label label) {
        NodeDataMask nodeMask = new NodeDataMask(mask);
        buildSuccessorProperties(graph, nodeMask, nodeBuilder, src, dst, label);
    }
}

/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Message;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.SWHID;
import org.softwareheritage.graph.SwhUnidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TraverseNodesPropertiesTest extends TraversalServiceTest {
    private TraversalRequest.Builder getTraversalRequestBuilder(SWHID src) {
        return TraversalRequest.newBuilder().addSrc(src.toString());
    }

    private void checkHasAllFields(Message m) {
        for (Descriptors.FieldDescriptor fd : m.getAllFields().keySet()) {
            assertTrue(m.hasField(fd));
        }
    }

    private void checkHasAllFieldsOfType(Node node) {
        if (node.hasCnt()) {
            checkHasAllFields(node.getCnt());
        }
        if (node.hasRev()) {
            checkHasAllFields(node.getRev());
        }
        if (node.hasRel()) {
            checkHasAllFields(node.getRel());
        }
        if (node.hasOri()) {
            checkHasAllFields(node.getOri());
        }
    }

    private void checkSuccessors(SwhUnidirectionalGraph g, Node node) {
        HashMap<String, DirEntry[]> graphSuccessors = new HashMap<>();
        ArcLabelledNodeIterator.LabelledArcIterator it = g.labelledSuccessors(g.getNodeId(new SWHID(node.getSwhid())));
        long succ;
        while ((succ = it.nextLong()) != -1) {
            graphSuccessors.put(g.getSWHID(succ).toString(), (DirEntry[]) it.label().get());
        }

        assertEquals(node.getSuccessorList().stream().map(Successor::getSwhid).collect(Collectors.toSet()),
                graphSuccessors.keySet());

        for (Successor successor : node.getSuccessorList()) {
            DirEntry[] expectedArray = graphSuccessors.get(successor.getSwhid());
            HashMap<String, Integer> expectedLabels = new HashMap<>();
            for (DirEntry dirEntry : expectedArray) {
                expectedLabels.put(new String(g.getLabelName(dirEntry.filenameId)), dirEntry.permission);
            }
            for (EdgeLabel edgeLabel : successor.getLabelList()) {
                assertTrue(expectedLabels.containsKey(edgeLabel.getName().toStringUtf8()));
                if (edgeLabel.getPermission() > 0) {
                    assertEquals(edgeLabel.getPermission(), expectedLabels.get(edgeLabel.getName().toStringUtf8()));
                }
            }
        }
    }

    @Test
    public void forwardFromRoot() {
        ArrayList<Node> response = new ArrayList<>();
        client.traverse(getTraversalRequestBuilder(new SWHID(TEST_ORIGIN_ID)).build()).forEachRemaining(response::add);
        for (Node node : response) {
            checkHasAllFieldsOfType(node);
            checkSuccessors(g.getForwardGraph(), node);
        }
    }

    @Test
    public void backwardFromLeaf() {
        ArrayList<Node> response = new ArrayList<>();
        client.traverse(getTraversalRequestBuilder(fakeSWHID("cnt", 4)).setDirection(GraphDirection.BACKWARD).build())
                .forEachRemaining(response::add);
        for (Node node : response) {
            checkHasAllFieldsOfType(node);
            checkSuccessors(g.getBackwardGraph(), node);
        }
    }

    @Test
    public void forwardFromRootMaskedLabels() {
        ArrayList<Node> response = new ArrayList<>();
        client.traverse(getTraversalRequestBuilder(new SWHID(TEST_ORIGIN_ID))
                .setMask(FieldMask.newBuilder().addPaths("successor.swhid").addPaths("swhid").build()).build())
                .forEachRemaining(response::add);
        for (Node node : response) {
            HashSet<String> graphSuccessors = new HashSet<>();
            ArcLabelledNodeIterator.LabelledArcIterator it = g
                    .labelledSuccessors(g.getNodeId(new SWHID(node.getSwhid())));
            long succ;
            while ((succ = it.nextLong()) != -1) {
                graphSuccessors.add(g.getSWHID(succ).toString());
            }

            assertEquals(node.getSuccessorList().stream().map(Successor::getSwhid).collect(Collectors.toSet()),
                    graphSuccessors);
        }
    }
}

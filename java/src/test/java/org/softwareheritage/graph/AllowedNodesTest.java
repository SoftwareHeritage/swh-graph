package org.softwareheritage.graph;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class AllowedNodesTest extends GraphTest {
    void assertNodeRestriction(AllowedNodes nodes, Set<Node.Type> expectedAllowed) {
        Node.Type[] nodeTypes = Node.Type.values();
        for (Node.Type t : nodeTypes) {
            boolean isAllowed = nodes.isAllowed(t);
            boolean isExpected = expectedAllowed.contains(t);
            Assertions.assertEquals(isAllowed, isExpected, "Node type: " + t);
        }
    }

    @Test
    public void dirCntNodes() {
        AllowedNodes edges = new AllowedNodes("dir,cnt");
        Set<Node.Type> expected = Set.of(Node.Type.DIR, Node.Type.CNT);
        assertNodeRestriction(edges, expected);
    }

    @Test
    public void revDirNodes() {
        AllowedNodes edges = new AllowedNodes("rev,dir");
        Set<Node.Type> expected = Set.of(Node.Type.DIR, Node.Type.REV);
        assertNodeRestriction(edges, expected);
    }

    @Test
    public void relSnpCntNodes() {
        AllowedNodes edges = new AllowedNodes("rel,snp,cnt");
        Set<Node.Type> expected = Set.of(Node.Type.REL, Node.Type.SNP, Node.Type.CNT);
        assertNodeRestriction(edges, expected);
    }

    @Test
    public void allNodes() {
        AllowedNodes edges = new AllowedNodes("*");
        Set<Node.Type> expected = Set.of(Node.Type.REL, Node.Type.SNP, Node.Type.CNT, Node.Type.DIR, Node.Type.REV,
                Node.Type.ORI);
        assertNodeRestriction(edges, expected);
    }

    @Test
    public void noNodes() {
        AllowedNodes edges = new AllowedNodes("");
        Set<Node.Type> expected = Set.of();
        assertNodeRestriction(edges, expected);
    }
}

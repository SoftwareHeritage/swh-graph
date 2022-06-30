/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class AllowedEdgesTest extends GraphTest {
    static class EdgeType {
        SwhType src;
        SwhType dst;

        public EdgeType(SwhType src, SwhType dst) {
            this.src = src;
            this.dst = dst;
        }

        @Override
        public boolean equals(Object otherObj) {
            if (otherObj == this)
                return true;
            if (!(otherObj instanceof EdgeType))
                return false;

            EdgeType other = (EdgeType) otherObj;
            return src == other.src && dst == other.dst;
        }
    }

    void assertEdgeRestriction(AllowedEdges edges, ArrayList<EdgeType> expectedAllowed) {
        SwhType[] nodeTypes = SwhType.values();
        for (SwhType src : nodeTypes) {
            for (SwhType dst : nodeTypes) {
                EdgeType edge = new EdgeType(src, dst);
                boolean isAllowed = edges.isAllowed(src, dst);
                boolean isExpected = false;
                for (EdgeType expected : expectedAllowed) {
                    if (expected.equals(edge)) {
                        isExpected = true;
                        break;
                    }
                }

                Assertions.assertEquals(isAllowed, isExpected, "Edge type: " + src + " -> " + dst);
            }
        }
    }

    @Test
    public void dirToDirDirToCntEdges() {
        AllowedEdges edges = new AllowedEdges("dir:dir,dir:cnt");
        ArrayList<EdgeType> expected = new ArrayList<>();
        expected.add(new EdgeType(SwhType.DIR, SwhType.DIR));
        expected.add(new EdgeType(SwhType.DIR, SwhType.CNT));
        assertEdgeRestriction(edges, expected);
    }

    @Test
    public void relToRevRevToRevRevToDirEdges() {
        AllowedEdges edges = new AllowedEdges("rel:rev,rev:rev,rev:dir");
        ArrayList<EdgeType> expected = new ArrayList<>();
        expected.add(new EdgeType(SwhType.REL, SwhType.REV));
        expected.add(new EdgeType(SwhType.REV, SwhType.REV));
        expected.add(new EdgeType(SwhType.REV, SwhType.DIR));
        assertEdgeRestriction(edges, expected);
    }

    @Test
    public void revToAllDirToDirEdges() {
        AllowedEdges edges = new AllowedEdges("rev:*,dir:dir");
        ArrayList<EdgeType> expected = new ArrayList<>();
        for (SwhType dst : SwhType.values()) {
            expected.add(new EdgeType(SwhType.REV, dst));
        }
        expected.add(new EdgeType(SwhType.DIR, SwhType.DIR));
        assertEdgeRestriction(edges, expected);
    }

    @Test
    public void allToCntEdges() {
        AllowedEdges edges = new AllowedEdges("*:cnt");
        ArrayList<EdgeType> expected = new ArrayList<>();
        for (SwhType src : SwhType.values()) {
            expected.add(new EdgeType(src, SwhType.CNT));
        }
        assertEdgeRestriction(edges, expected);
    }

    @Test
    public void allEdges() {
        AllowedEdges edges = new AllowedEdges("*:*");
        ArrayList<EdgeType> expected = new ArrayList<>();
        for (SwhType src : SwhType.values()) {
            for (SwhType dst : SwhType.values()) {
                expected.add(new EdgeType(src, dst));
            }
        }
        assertEdgeRestriction(edges, expected);

        // Special null value used to quickly bypass edge check when no restriction
        AllowedEdges edges2 = new AllowedEdges("*");
        Assertions.assertNull(edges2.restrictedTo);
    }

    @Test
    public void noEdges() {
        AllowedEdges edges = new AllowedEdges("");
        AllowedEdges edges2 = new AllowedEdges(null);
        ArrayList<EdgeType> expected = new ArrayList<>();
        assertEdgeRestriction(edges, expected);
        assertEdgeRestriction(edges2, expected);
    }
}

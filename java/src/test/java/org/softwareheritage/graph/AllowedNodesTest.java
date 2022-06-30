/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class AllowedNodesTest extends GraphTest {
    void assertNodeRestriction(AllowedNodes nodes, Set<SwhType> expectedAllowed) {
        SwhType[] nodeTypes = SwhType.values();
        for (SwhType t : nodeTypes) {
            boolean isAllowed = nodes.isAllowed(t);
            boolean isExpected = expectedAllowed.contains(t);
            Assertions.assertEquals(isAllowed, isExpected, "Node type: " + t);
        }
    }

    @Test
    public void dirCntNodes() {
        AllowedNodes edges = new AllowedNodes("dir,cnt");
        Set<SwhType> expected = Set.of(SwhType.DIR, SwhType.CNT);
        assertNodeRestriction(edges, expected);
    }

    @Test
    public void revDirNodes() {
        AllowedNodes edges = new AllowedNodes("rev,dir");
        Set<SwhType> expected = Set.of(SwhType.DIR, SwhType.REV);
        assertNodeRestriction(edges, expected);
    }

    @Test
    public void relSnpCntNodes() {
        AllowedNodes edges = new AllowedNodes("rel,snp,cnt");
        Set<SwhType> expected = Set.of(SwhType.REL, SwhType.SNP, SwhType.CNT);
        assertNodeRestriction(edges, expected);
    }

    @Test
    public void allNodes() {
        AllowedNodes edges = new AllowedNodes("*");
        Set<SwhType> expected = Set.of(SwhType.REL, SwhType.SNP, SwhType.CNT, SwhType.DIR, SwhType.REV, SwhType.ORI);
        assertNodeRestriction(edges, expected);
    }

    @Test
    public void noNodes() {
        AllowedNodes edges = new AllowedNodes("");
        Set<SwhType> expected = Set.of();
        assertNodeRestriction(edges, expected);
    }
}

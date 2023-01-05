/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StatsTest extends TraversalServiceTest {
    @Test
    public void testStats() {
        StatsResponse stats = client.stats(StatsRequest.getDefaultInstance());
        assertEquals(stats.getNumNodes(), 24);
        assertEquals(stats.getNumEdges(), 28);
        assertEquals(stats.getIndegreeMin(), 0);
        assertEquals(stats.getIndegreeMax(), 4);
        assertEquals(stats.getOutdegreeMin(), 0);
        assertEquals(stats.getOutdegreeMax(), 3);
    }
}

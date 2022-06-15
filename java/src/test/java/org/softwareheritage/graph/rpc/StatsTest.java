package org.softwareheritage.graph.rpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class StatsTest extends TraversalServiceTest {
    @Test
    public void testStats() {
        StatsResponse stats = client.stats(StatsRequest.getDefaultInstance());
        assertEquals(stats.getNumNodes(), 21);
        assertEquals(stats.getNumEdges(), 23);
        assertEquals(stats.getIndegreeMin(), 0);
        assertEquals(stats.getIndegreeMax(), 3);
        assertEquals(stats.getOutdegreeMin(), 0);
        assertEquals(stats.getOutdegreeMax(), 3);
    }
}

package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2011-2017 Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.Transform.ArcFilter;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.util.XorShift1024StarRandom;

import java.io.IOException;

import org.junit.Test;

public class TransformSlowTest extends WebGraphTestCase {

	@Test
	public void testTranspose() throws IOException {
		final ImmutableGraph graph = new BVGraphSlowTest.BigGraph(3L << 28, 1 << 2);
		assertEquals(graph, Transform.transposeOffline(Transform.transposeOffline(graph, 1000000000), 1000000000));
	}

	@Test
	public void testSymmetrize() throws IOException {
		final ImmutableGraph graph = new BVGraphSlowTest.BigGraph(3L << 28, 1 << 2);
		assertEquals(Transform.symmetrizeOffline(graph, 1000000000), Transform.symmetrizeOffline(Transform.symmetrizeOffline(graph, 1000000000), 1000000000));
		assertEquals(Transform.symmetrizeOffline(graph, 1000000000), Transform.symmetrizeOffline(Transform.transposeOffline(graph, 1000000000), 1000000000));
	}

	@Test
	public void testMap() throws IOException {
		final ImmutableGraph graph = new BVGraphSlowTest.BigGraph((2L << 20) + 1, 1 << 10);
		final long[][] perm = Util.identity(graph.numNodes());
		LongBigArrays.shuffle(perm, new XorShift1024StarRandom(0));
		final long[][] inv = Util.invertPermutation(perm);
		assertEquals(graph, Transform.mapOffline(Transform.mapOffline(graph, perm, 1000000000), inv, 1000000000));
	}

	@Test
	public void testFilter() {
		final ImmutableGraph graph = new BVGraphSlowTest.BigGraph((2L << 20) + 1, 1 << 10);
		// Just testings that the basic implementation is OK.
		assertEquals(graph, Transform.filterArcs(graph, new ArcFilter() {
			@Override
			public boolean accept(long i, long t) {
				return true;
			}
		}));
	}
}

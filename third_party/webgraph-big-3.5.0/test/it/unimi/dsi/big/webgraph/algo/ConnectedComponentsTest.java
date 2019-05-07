package it.unimi.dsi.big.webgraph.algo;

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


import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.WebGraphTestCase;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

import org.junit.Test;


public class ConnectedComponentsTest extends WebGraphTestCase {
	public static void sameComponents(ImmutableGraph g) {
		StronglyConnectedComponentsTarjan stronglyConnectedComponents = StronglyConnectedComponentsTarjan.compute(g, false, new ProgressLogger());
		long[][] size2 = stronglyConnectedComponents.computeSizes();
		stronglyConnectedComponents.sortBySize(size2);

		for(int t = 0; t < 3; t++) {
			ConnectedComponents connectedComponents = ConnectedComponents.compute(g, t, new ProgressLogger());
			long[][] size = connectedComponents.computeSizes();
			connectedComponents.sortBySize(size);
			for(long i = g.numNodes(); i-- != 0;)
				for(long j = i; j-- != 0;)
					assert((LongBigArrays.get(connectedComponents.component, i) == LongBigArrays.get(connectedComponents.component, j))
							== (LongBigArrays.get(stronglyConnectedComponents.component, i) == LongBigArrays.get(stronglyConnectedComponents.component, j)));
		}
	}

	@Test
	public void testSmall() {
		sameComponents(ImmutableGraph.wrap(ArrayListMutableGraph.newBidirectionalCycle(40).immutableView()));
	}

	@Test
	public void testBinaryTree() {
		sameComponents(ImmutableGraph.wrap(Transform.symmetrize(ArrayListMutableGraph.newCompleteBinaryIntree(10).immutableView())));
	}

	@Test
	public void testErdosRenyi() {
		for(int size: new int[] { 10, 100, 1000 })
			for(int attempt = 0; attempt < 5; attempt++)
				sameComponents(ImmutableGraph.wrap(Transform.symmetrize(new ArrayListMutableGraph(new ErdosRenyiGraph(size, .001, attempt + 1, true)).immutableView())));
	}
}

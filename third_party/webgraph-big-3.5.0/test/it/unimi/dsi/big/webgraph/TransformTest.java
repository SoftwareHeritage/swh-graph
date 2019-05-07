package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2007-2017 Sebastiano Vigna
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.examples.IntegerTriplesArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledGraphTest;
import it.unimi.dsi.big.webgraph.labelling.GammaCodedIntLabel;
import it.unimi.dsi.big.webgraph.labelling.Label;
import it.unimi.dsi.big.webgraph.labelling.LabelSemiring;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class TransformTest extends WebGraphTestCase {

	@Test
	public void testMapExpand() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(4, false).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { 0, 2, 4, 6 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(7, new it.unimi.dsi.webgraph.Transform.ArcFilter() {
			@Override
			public boolean accept(int i, int j) {
				return i % 2 == 0 && j % 2 == 0 && i != j;
			}

		}).immutableView()), g2);

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { 0, 3, 3 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(4, new int[][] { { 0, 3 }, { 3, 0 }, { 3, 3 } }).immutableView()), g2);

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { 4, 4, 4 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(5, new int[][] { { 4, 4 } }).immutableView()), g2);

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { 6, 5, 4 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(7, new int[][] { { 6, 5 }, { 5, 4 }, { 4, 6 } }).immutableView()), g2);

	}

	@Test
	public void testMapPermutation() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { 2, 1, 0 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 2 }, { 2, 1 }, { 1, 0 } }).immutableView()), g2);
	}

	@Test
	public void testInjective() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 1 }, { 1, 2 }, { 0, 2 } }).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { 2, -1, 0 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 2, 0 } }).immutableView()), g2);
	}

	@Test
	public void testMapCollapse() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { 0, 0, 0 }), 10);
		assertGraph(g2);
		assertEquals(1, g2.numNodes());
	}

	@Test
	public void testMapClear() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { -1, -1, -1 }), 10);
		assertGraph(g2);
		assertEquals(0, g2.numNodes());
	}

	@Test
	public void testMapKeepMiddle() throws IOException {
		ImmutableGraph g;
		ImmutableGraph g2;

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { -1, 0, -1 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(ArrayListMutableGraph.newCompleteGraph(1, false).immutableView()), g2);

		g = ImmutableGraph.wrap(ArrayListMutableGraph.newDirectedCycle(3).immutableView());
		g2 = Transform.mapOffline(g, LongBigArrays.wrap(new long[] { -1, 2, -1 }), 10);
		assertGraph(g2);
		assertEquals(ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] {}).immutableView()), g2);
	}

	@Test
	public void testLex() {
		ImmutableGraph g = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 2 }, { 1, 1 }, { 1, 2 }, { 2, 0 }, { 2, 1 }, { 2, 2 } }).immutableView());
		long[][] p = Transform.lexicographicalPermutation(g);
		assertArrayEquals(LongBigArrays.wrap(new long[] { 0, 1, 2 }), p);

		g = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][] { { 0, 0 }, { 0, 1 }, { 0, 2 }, { 1, 1 }, { 1, 2 }, { 2, 2 } }).immutableView());
		p = Transform.lexicographicalPermutation(g);
		assertArrayEquals(LongBigArrays.wrap(new long[] { 2, 1, 0 }), p);
	}


	@Test
	public void testFilters() throws IllegalArgumentException, SecurityException {
		ImmutableGraph graph = ImmutableGraph.wrap(new ArrayListMutableGraph(6,
				new int[][] {
						{ 0, 1 },
						{ 0, 2 },
						{ 1, 1 },
						{ 1, 3 },
						{ 2, 1 },
						{ 4, 5 },
				}
		).immutableView());

		ImmutableGraph filtered = Transform.filterArcs(graph, new Transform.ArcFilter() {
			@Override
			public boolean accept(long i, long j) {
				return i < j;
			}
		}, null);

		assertGraph(filtered);

		NodeIterator nodeIterator = filtered.nodeIterator();
		LazyLongIterator iterator;
		assertTrue(nodeIterator.hasNext());
		assertEquals(0, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(1, iterator.nextLong());
		assertEquals(2, iterator.nextLong());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(1, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(3, iterator.nextLong());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(2, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(3, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertEquals(4, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(5, iterator.nextLong());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(5, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertFalse(nodeIterator.hasNext());
	}


	@Test
	public void testLabelledFilters() throws IllegalArgumentException, SecurityException, IOException {
		IntegerTriplesArcLabelledImmutableGraph graph = new IntegerTriplesArcLabelledImmutableGraph(
				new int[][] {
						{ 0, 1, 2 },
						{ 0, 2, 3 },
						{ 1, 1, 4 },
						{ 1, 3, 5 },
						{ 2, 1, 6 },
						{ 4, 5, 7 },
				}
		);

		ArcLabelledImmutableGraph filtered = Transform.filterArcs(graph, new Transform.LabelledArcFilter() {
			@Override
			public boolean accept(long i, long j, Label label) {
				return i < j;
			}
		}, null);

		ArcLabelledNodeIterator nodeIterator = filtered.nodeIterator();
		LabelledArcIterator iterator;
		assertTrue(nodeIterator.hasNext());
		assertEquals(0, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(1, iterator.nextLong());
		assertEquals(2, iterator.label().getInt());
		assertEquals(2, iterator.nextLong());
		assertEquals(3, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(1, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(3, iterator.nextLong());
		assertEquals(5, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(2, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(3, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertEquals(4, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(5, iterator.nextLong());
		assertEquals(7, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		assertTrue(nodeIterator.hasNext());
		assertEquals(5, nodeIterator.nextLong());
		iterator = nodeIterator.successors();
		assertEquals(-1, iterator.nextLong());
		assertFalse(nodeIterator.hasNext());

		File file = BitStreamArcLabelledGraphTest.storeTempGraph(graph);
		ArcLabelledImmutableGraph graph2 = ArcLabelledImmutableGraph.load(file.toString());

		filtered = Transform.filterArcs(graph2, new Transform.LabelledArcFilter() {
			@Override
			public boolean accept(long i, long j, Label label) {
				return i < j;
			}
		}, null);

		iterator = filtered.successors(0);
		assertEquals(1, iterator.nextLong());
		assertEquals(2, iterator.label().getInt());
		assertEquals(2, iterator.nextLong());
		assertEquals(3, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(1);
		assertEquals(3, iterator.nextLong());
		assertEquals(5, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(2);
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(3);
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(4);
		assertEquals(5, iterator.nextLong());
		assertEquals(7, iterator.label().getInt());
		assertEquals(-1, iterator.nextLong());
		iterator = filtered.successors(5);
		assertEquals(-1, iterator.nextLong());

	}

	@Test
	public void testCompose() {
		ImmutableGraph g0 = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][]  { { 0, 1 }, { 0, 2 } }).immutableView());
		ImmutableGraph g1 = ImmutableGraph.wrap(new ArrayListMutableGraph(3, new int[][]  { { 1, 0 }, { 2, 1 } }).immutableView());

		ImmutableGraph c = Transform.compose(g0, g1);

		NodeIterator n = c.nodeIterator();
		assertTrue(n.hasNext());
		assertEquals(0, n.nextLong());
		LazyLongIterator i = n.successors();
		assertEquals(0, i.nextLong());
		assertEquals(1, i.nextLong());
		assertEquals(-1, i.nextLong());
		assertEquals(1, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertTrue(n.hasNext());
		assertEquals(2, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertFalse(n.hasNext());

		assertEquals(c, c.copy());
		assertEquals(c.copy(), c);
	}

	@Test
	public void testLabelledCompose() throws IllegalArgumentException, SecurityException, IOException {
		File file = BitStreamArcLabelledGraphTest.storeTempGraph(new IntegerTriplesArcLabelledImmutableGraph(
				new int[][] {
						{ 0, 1, 2 },
						{ 0, 2, 10 },
						{ 0, 3, 1 },
						{ 1, 2, 4 },
						{ 3, 2, 1 },
				}
		));
		ArcLabelledImmutableGraph graph = ArcLabelledImmutableGraph.load(file.toString());

		ArcLabelledImmutableGraph composed = Transform.compose(graph, graph, new LabelSemiring() {
			private final GammaCodedIntLabel one = new GammaCodedIntLabel("FOO");
			private final GammaCodedIntLabel zero = new GammaCodedIntLabel("FOO");
			{
				one.value = 0;
				zero.value = Integer.MAX_VALUE;
			}

			@Override
			public Label add(Label first, Label second) {
				GammaCodedIntLabel result = new GammaCodedIntLabel("FOO");
				result.value = Math.min(first.getInt(), second.getInt());
				return result;
			}

			@Override
			public Label multiply(Label first, Label second) {
				GammaCodedIntLabel result = new GammaCodedIntLabel("FOO");
				result.value = first.getInt() + second.getInt();
				return result;
			}

			@Override
			public Label one() {
				return one;
			}

			@Override
			public Label zero() {
				return zero;
			}
		});

		ArcLabelledNodeIterator n = composed.nodeIterator();
		assertTrue(n.hasNext());
		assertEquals(0, n.nextLong());
		LabelledArcIterator i = n.successors();
		assertEquals(2, i.nextLong());
		assertEquals(2, i.label().getInt());
		assertEquals(-1, i.nextLong());
		assertEquals(1, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertTrue(n.hasNext());
		assertEquals(2, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertTrue(n.hasNext());
		assertEquals(3, n.nextLong());
		i = n.successors();
		assertEquals(-1, i.nextLong());
		assertFalse(n.hasNext());
	}

	@Test
	public void testMapOffline() throws IOException {
		ImmutableSequentialGraph g = new ErdosRenyiGraph(10, .5, 0, false);
		long[][] perm = Util.identity((long)g.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		long[][] inv = Util.invertPermutation(perm);
		ImmutableGraph gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100));
		assertEquals(ImmutableGraph.wrap(g), Transform.mapOffline(Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100), inv, 100));
		assertEquals(gm, gm.copy());

		perm = Util.identity((long)g.numNodes());
		LongBigArrays.set(perm, LongBigArrays.length(perm) - 1, -1);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100));
		assertEquals(gm, gm.copy());

		perm = Util.identity((long)g.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		LongBigArrays.set(perm, 0, -1); LongBigArrays.set(perm, LongBigArrays.length(perm) / 2, -1);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100));
		assertEquals(gm, gm.copy());

		perm = Util.identity((long)g.numNodes());
		LongBigArrays.set(perm, 1, 0); LongBigArrays.set(perm, LongBigArrays.length(perm) - 2, LongBigArrays.length(perm) - 1);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 100));
		assertEquals(gm, gm.copy());

		g = new ErdosRenyiGraph(1000, .2, 0, false);
		perm = Util.identity((long)g.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		inv = Util.invertPermutation(perm);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 1000000));
		assertEquals(ImmutableGraph.wrap(g), Transform.mapOffline(Transform.mapOffline(ImmutableGraph.wrap(g), perm, 10000), inv, 10000));
		assertEquals(gm, gm.copy());

		perm = Util.identity((long)g.numNodes());
		LongBigArrays.shuffle(perm, new XoRoShiRo128PlusRandom(0));
		LongBigArrays.set(perm, 0, -1); LongBigArrays.set(perm, LongBigArrays.length(perm) / 2, -1);
		LongBigArrays.set(perm, LongBigArrays.length(perm) / 4, -1); LongBigArrays.set(perm, 3 * LongBigArrays.length(perm) / 4, -1);
		gm = Transform.mapOffline(ImmutableGraph.wrap(new ArrayListMutableGraph(g).immutableView()), perm, 100);
		assertEquals(gm, Transform.mapOffline(ImmutableGraph.wrap(g), perm, 1000000));
		assertEquals(gm, gm.copy());
	}

	@Test
	public void testSymmetrizeOffline() throws IOException {
		ImmutableGraph g = ImmutableGraph.wrap(new ErdosRenyiGraph(5, .5, 0, false));
		ImmutableGraph gs = Transform.symmetrizeOffline(g, 10);
		assertEquals(gs, Transform.symmetrizeOffline(g, 5));
		assertEquals(gs, Transform.symmetrizeOffline(Transform.symmetrizeOffline(g, 100), 5));
		assertEquals(gs, gs.copy());

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(100, .5, 0, false));
		gs = Transform.symmetrizeOffline(g, 100);
		assertEquals(gs, Transform.symmetrizeOffline(g, 1000));
		assertEquals(gs, Transform.symmetrizeOffline(Transform.symmetrizeOffline(g, 100), 10000));
		assertEquals(gs, gs.copy());

		g = ImmutableGraph.wrap(new ErdosRenyiGraph(1000, .2, 0, false));
		gs = Transform.symmetrizeOffline(g, 1000);
		assertEquals(gs, Transform.symmetrizeOffline(g, 10000));
		assertEquals(gs, Transform.symmetrizeOffline(Transform.symmetrizeOffline(g, 10000), 100000));
		assertEquals(gs, gs.copy());
	}
}

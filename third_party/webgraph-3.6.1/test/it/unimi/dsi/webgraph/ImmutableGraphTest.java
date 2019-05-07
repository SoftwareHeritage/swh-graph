package it.unimi.dsi.webgraph;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.util.XoRoShiRo128PlusRandomGenerator;
import it.unimi.dsi.webgraph.Transform.ArcFilter;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

/*
   Copyright (C) 2010-2017 Paolo Boldi, Sebastiano Vigna

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.


 */



public class ImmutableGraphTest {

	public final static boolean DEBUG = false;

	public void assertSplitIterators(final String graphFilename) throws IOException {
		XoRoShiRo128PlusRandomGenerator r = new XoRoShiRo128PlusRandomGenerator(0);
		int i = 0;
		for (;;) {
			ImmutableGraph g = ImmutableGraph.loadOffline(graphFilename);
			switch (i) {
			case 0:
				WebGraphTestCase.assertSplitIterator(g, 4);
				i++; break;
			case 1:
				WebGraphTestCase.assertSplitIterator(g, 1);
				i++; break;
			case 2:
				if (g.numNodes() / 4 > 0) WebGraphTestCase.assertSplitIterator(g, Math.max(1, g.numNodes() / 4));
				i++; break;
			case 3:
				WebGraphTestCase.assertSplitIterator(g, g.numNodes());
				i++; break;
			case 4:
				WebGraphTestCase.assertSplitIterator(g, Math.max(1, g.numNodes() / (r.nextInt(10) + 1)));
				i++; break;
			case 5:
				WebGraphTestCase.assertSplitIterator(g, r.nextInt(10) + 1);
				i++; break;
			default:
				return;
			}
		}
	}

	@Test
	public void testBVGraphSplitIteratorsOffline() throws IllegalArgumentException, SecurityException, IOException {
		for (int size: new int[] { 5, 10, 100 })
			for (double p: new double[] { .1, .3, .5, .9 }) {
				ErdosRenyiGraph eg = new ErdosRenyiGraph(size, p, true);
				File graphFile = BVGraphTest.storeTempGraph(eg);
				ImmutableGraph graph;
				graph = ImmutableGraph.load(graphFile.getAbsolutePath());
				WebGraphTestCase.assertGraph(graph);
				graph = ImmutableGraph.loadOffline(graphFile.getAbsolutePath());
				WebGraphTestCase.assertGraph(graph);
				assertSplitIterators(graphFile.getAbsolutePath());
				graphFile.delete();
			}
	}

	@Test
	public void testTransformFilterSplitIterators() throws IllegalArgumentException, SecurityException, IOException {
		XoRoShiRo128PlusRandomGenerator r = new XoRoShiRo128PlusRandomGenerator(0);
		for (int size: new int[] { 5, 10, 100 })
			for (double p: new double[] { .1, .3, .5, .9 }) {
				ErdosRenyiGraph eg = new ErdosRenyiGraph(size, p, true);
				File graphFile = BVGraphTest.storeTempGraph(eg);
				ImmutableGraph graph;
				graph = ImmutableGraph.load(graphFile.getAbsolutePath());
				ImmutableGraph filteredArcs = Transform.filterArcs(graph, new ArcFilter() {
					@Override
					public boolean accept(int i, int j) {
						return i % 3 == 1 && j % 5 > 3;
					}
				});
				WebGraphTestCase.assertSplitIterator(filteredArcs, Math.max(1, r.nextInt(size)));
			}
	}

	@Test
	public void testImmutableSubgraphSplitIterators() throws IllegalArgumentException, SecurityException, IOException {
		XoRoShiRo128PlusRandomGenerator r = new XoRoShiRo128PlusRandomGenerator(2);
		for (int size: new int[] { 5, 10, 100 })
			for (double p: new double[] { .1, .3, .5, .9 }) {
				ErdosRenyiGraph eg = new ErdosRenyiGraph(size, p, true);
				File graphFile = BVGraphTest.storeTempGraph(eg);
				ImmutableGraph graph;
				graph = ImmutableGraph.load(graphFile.getAbsolutePath());

				IntSet nodeSet = new IntOpenHashSet();
				for (int i = 0; i < size; i++) if (r.nextBoolean()) nodeSet.add(i);
				int[] nodeArray = nodeSet.toIntArray();
				Arrays.sort(nodeArray);
				WebGraphTestCase.assertSplitIterator(new ImmutableSubgraph(graph, nodeArray), Math.max(1, r.nextInt(nodeArray.length)));
			}
	}

	@Test
	public void testUnionImmutableGraphSplitIterators() throws IllegalArgumentException, SecurityException, IOException {
		XoRoShiRo128PlusRandomGenerator r = new XoRoShiRo128PlusRandomGenerator(0);
		for (int size: new int[] { 5, 10, 100 })
			for (double p: new double[] { .1, .3, .5, .9 }) {
				ErdosRenyiGraph eg0 = new ErdosRenyiGraph(size, p, true);
				File graphFile0 = BVGraphTest.storeTempGraph(eg0);
				ImmutableGraph graph0;
				graph0 = ImmutableGraph.load(graphFile0.getAbsolutePath());
				ErdosRenyiGraph eg1 = new ErdosRenyiGraph(size, p, true);
				File graphFile1 = BVGraphTest.storeTempGraph(eg1);
				ImmutableGraph graph1;
				graph1 = ImmutableGraph.load(graphFile1.getAbsolutePath());
				WebGraphTestCase.assertSplitIterator(new UnionImmutableGraph(graph0, graph1), Math.max(1, r.nextInt(graph0.numNodes() + graph1.numNodes())));
			}
	}
}

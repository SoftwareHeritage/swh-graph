package it.unimi.dsi.law.rank;

import java.io.File;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.helpers.NOPLogger;

/*
 *  Copyright (C) 2010-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.doubles.AbstractDoubleList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.law.TestUtil;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;


// RELEASE-STATUS: DIST

public class PageRankPushTest {
	private final static String GRAPH_NAME = "test50-.6-7-3-2-10-graph";
	private static String baseNameGraph;
	private static ImmutableGraph graph;
	private static File file;

	@BeforeClass
	public static void setUp() throws Exception {
		baseNameGraph = TestUtil.getTestFile(PageRankPushTest.class, GRAPH_NAME, false);
		file = File.createTempFile(PageRankPushTest.class.getSimpleName(), "graph");
		BVGraph.store(Transform.filterArcs(Transform.symmetrize(ImmutableGraph.load(baseNameGraph)), Transform.NO_LOOPS, null), file.toString());
		graph = ImmutableGraph.load(file.toString());
	}

	@AfterClass
	public static void tearDown() throws Exception {
		file.delete();
		new File(file + BVGraph.PROPERTIES_EXTENSION).delete();
		new File(file + BVGraph.GRAPH_EXTENSION).delete();
		new File(file + BVGraph.OFFSETS_EXTENSION).delete();
	}

	@Test
	public void testRank() throws Exception {
		final ImmutableGraph transpose = Transform.transpose(graph);
		final PageRankParallelGaussSeidel prPGS = new PageRankParallelGaussSeidel(transpose, NOPLogger.NOP_LOGGER);
		final int n = graph.numNodes();

		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 1000) {
			for(boolean l1: new boolean[] { true, false }) {
				for(boolean fifo: new boolean[] { true, false }) {
					final PageRankPush pageRankPush = new PageRankPush(graph, NOPLogger.NOP_LOGGER, fifo);

					for (int root = 0; root < n; root++) {
						final int r = root;
						final DoubleList preference = new AbstractDoubleList() {
							@Override
							public double getDouble(int u) {
								return u == r ? 1 : 0;
							}

							@Override
							public int size() {
								return n;
							}
						};

						// Compute norm vector for all alpha < .99
						final PowerSeries w = new PowerSeries(transpose, NOPLogger.NOP_LOGGER);
						w.markovian = true;
						w.alpha = .999;
						w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

						prPGS.normVector(w.previousRank, w.maxRatio);

						for(double alpha: new double[] { 0, .25, .5, .75, .99 }) {
							System.err.println("root=" + root + ", threshold=" + threshold + ", alpha=" + alpha + " fifo=" + fifo + ", l1=" + l1);

							prPGS.preference = preference;
							prPGS.pseudoRank = true;

							pageRankPush.alpha = prPGS.alpha = alpha;
							pageRankPush.root = root;
							pageRankPush.threshold = threshold / 10;

							if (l1) pageRankPush.stepUntil(new PageRankPush.L1NormStoppingCritertion());
							else pageRankPush.stepUntil(new PageRankPush.EmptyQueueStoppingCritertion());

							final double[] rank = new double[n];

							for (int i = pageRankPush.node2Seen.size(); i-- != 0;) rank[pageRankPush.seen2Node[i]] = pageRankPush.rank[i] / (1 - pageRankPush.backToRoot);
							prPGS.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / n));
							Assert.assertEquals(0, Norm.L_1.compute(prPGS.rank, rank), 2 * threshold);

							for (int i = pageRankPush.node2Seen.size(); i-- != 0;) rank[pageRankPush.seen2Node[i]] = pageRankPush.rank[i] / pageRankPush.pNorm;
							Norm.L_1.normalize(prPGS.rank, 1);
							Assert.assertEquals(0, Norm.L_1.compute(prPGS.rank, rank), 2 * threshold);

						}
					}
				}
			}
		}
	}
}

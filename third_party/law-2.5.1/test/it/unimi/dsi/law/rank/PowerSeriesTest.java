package it.unimi.dsi.law.rank;

/*
 *  Copyright (C) 2011-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import static java.lang.Math.pow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;



//RELEASE-STATUS: DIST

public class PowerSeriesTest {

	@Test
	public void testCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for (int size : new int[] { 100, 1000, 10000 }) {
				final PowerSeries katz = new PowerSeries(ArrayListMutableGraph.newDirectedCycle(size).immutableView());
				katz.alpha = 1. / 2;
				katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
				for (int i = katz.graph.numNodes(); i-- != 0;)
					assertEquals(1. / (1 - katz.alpha), katz.rank[i] / katz.scale, threshold);
			}
		}
	}

	@Test
	public void testClique() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for (int size : new int[] { 10, 100, 1000 }) {
				final PowerSeries katz = new PowerSeries(ArrayListMutableGraph.newCompleteGraph(size, false).immutableView());
				katz.alpha = 1. / (2 * size);
				katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
				for (int i = katz.graph.numNodes(); i-- != 0;)
					assertEquals(1. / (1 - katz.alpha * (size - 1)), katz.rank[i] / katz.scale, threshold);
			}
		}
	}

	@Test
	public void testCliqueBibridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for (int p : new int[] { 10, 50, 100 }) {
				for (int k : new int[] { 10, 50, 100 }) {
					ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k, new int[][] {});
					for (int i = 0; i < k; i++)
						for (int j = 0; j < k; j++)
							if (i != j) mg.addArc(i, j);
					// Note the transposition
					for (int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					mg.addArc(k - 1, k);
					mg.addArc(k, k - 1);
					ImmutableGraph g = mg.immutableView();

					PowerSeries katz = new PowerSeries(g);
					final double alpha = 1. / (k + 1);
					katz.alpha = alpha;

					katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 100));
					final double[] rank = katz.rank;
					final double normalization = 1 / katz.scale;
					for(int i = rank.length; i-- != 0;) katz.rank[i] *= normalization;
					final double r = rank[k - 1];
					for (int i = k - 1; i-- != 0;)
						assertEquals((1 + alpha * r) / (1 - (k - 2) * alpha), rank[i], threshold * normalization);
					assertEquals(1 / (1 - alpha) + alpha * r / (1 - Math.pow(alpha, p)), rank[k], threshold * normalization);
					for (int d = 1; d < p; d++)
						assertEquals(1 / (1 - alpha) + Math.pow(alpha, d + 1) * r / (1 - Math.pow(alpha, p)), rank[k + d], threshold * normalization);


					katz.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
						final int curr = nodeIterator.nextInt();
						double t = 0;
						LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) t += katz.previousRank[s];
						assertTrue(t / katz.previousRank[curr] < 1 / katz.alpha);
					}

					katz.alpha = .5 / (k + 1);
					katz.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
						final int curr = nodeIterator.nextInt();
						double t = 0;
						LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) t += katz.previousRank[s];
						assertTrue(t / katz.previousRank[curr] < 1 / katz.alpha);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueBackbridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for (int p : new int[] { 10, 50, 100 }) {
				for (int k : new int[] { 10, 50, 100 }) {
					ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k, new int[][] {});
					for (int i = 0; i < k; i++)
						for (int j = 0; j < k; j++)
							if (i != j) mg.addArc(i, j);
					// Note the transposition
					for (int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					mg.addArc(k - 1, k);
					ImmutableGraph g = mg.immutableView();

					PowerSeries katz = new PowerSeries(g);
					final double alpha = 1. / (k + 1);
					katz.alpha = alpha;

					katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 100));
					final double[] rank = katz.rank;
					final double normalization = 1 / katz.scale;
					for(int i = rank.length; i-- != 0;) katz.rank[i] *= normalization;
					for (int i = k - 1; i-- != 0;)
						assertEquals(-1 / ((1 - alpha) * (alpha * alpha * (k - 1) + alpha * (k - 2) - 1)), rank[i], threshold * normalization);
					assertEquals((alpha * alpha * (k - 1) - alpha - 1) / ((1 - alpha) * (alpha * alpha * (k - 1) + alpha * (k - 2) - 1)), rank[k - 1], threshold * normalization);
					for (int d = 1; d < p; d++)
						assertEquals(1 / (1 - alpha), rank[k + d], threshold * normalization);


					katz.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
						final int curr = nodeIterator.nextInt();
						double t = 0;
						LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) t += katz.previousRank[s];
						assertTrue(t / katz.previousRank[curr] < 1 / katz.alpha);
					}

					katz.alpha = .5 / (k + 1);
					katz.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
						final int curr = nodeIterator.nextInt();
						double t = 0;
						LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) t += katz.previousRank[s];
						assertTrue(t / katz.previousRank[curr] < 1 / katz.alpha);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueForwardbridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for (int p : new int[] { 10, 50, 100 }) {
				for (int k : new int[] { 10, 50, 100 }) {
					ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k, new int[][] {});
					for (int i = 0; i < k; i++)
						for (int j = 0; j < k; j++)
							if (i != j) mg.addArc(i, j);
					// Note the transposition
					for (int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					mg.addArc(k, k - 1);
					ImmutableGraph g = mg.immutableView();

					PowerSeries katz = new PowerSeries(g);
					final double alpha = 1. / (k + 1);
					katz.alpha = alpha;

					katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 100));
					final double[] rank = katz.rank;
					final double normalization = 1 / katz.scale;
					for(int i = rank.length; i-- != 0;) katz.rank[i] *= normalization;
					for (int i = k; i-- != 0;)
						assertEquals(1 / (1 - (k - 1) * alpha), rank[i], threshold * normalization);
					for (int d = 0; d < p; d++)
						assertEquals(1 / (1 - alpha) + pow(alpha, d + 1) / ((1 - (k - 1) * alpha) * (1 - pow(alpha, p))), rank[k + d], threshold * normalization);

					katz.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
						final int curr = nodeIterator.nextInt();
						double t = 0;
						LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) t += katz.previousRank[s];
						assertTrue(t / katz.previousRank[curr] < 1 / katz.alpha);
					}

					katz.alpha = .5 / (k + 1);
					katz.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
						final int curr = nodeIterator.nextInt();
						double t = 0;
						LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) t += katz.previousRank[s];
						assertTrue(t / katz.previousRank[curr] < 1 / katz.alpha);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueNobridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for (int p : new int[] { 10, 50, 100 }) {
				for (int k : new int[] { 10, 50, 100 }) {
					ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k, new int[][] {});
					for (int i = 0; i < k; i++)
						for (int j = 0; j < k; j++)
							if (i != j) mg.addArc(i, j);
					// Note the transposition
					for (int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					ImmutableGraph g = mg.immutableView();

					PowerSeries katz = new PowerSeries(g);
					final double alpha = 1. / (k + 1);
					katz.alpha = alpha;

					katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 100));
					final double[] rank = katz.rank;
					final double normalization = 1 / katz.scale;
					for(int i = rank.length; i-- != 0;) katz.rank[i] *= normalization;
					for (int i = k; i-- != 0;)
						assertEquals(1 / (1 - (k - 1) * alpha), rank[i], threshold * normalization);
					for (int d = 0; d < p; d++)
						assertEquals(1 / (1 - alpha), rank[k + d], threshold * normalization);

					katz.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
						final int curr = nodeIterator.nextInt();
						double t = 0;
						LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) t += katz.previousRank[s];
						assertTrue(t / katz.previousRank[curr] < 1 / katz.alpha);
					}

					katz.alpha = .5 / (k + 1);
					katz.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
						final int curr = nodeIterator.nextInt();
						double t = 0;
						LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) t += katz.previousRank[s];
						assertTrue(t / katz.previousRank[curr] < 1 / katz.alpha);
					}
				}
			}
		}
	}


}

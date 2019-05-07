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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.math.linear.Array2DRowRealMatrix;
import org.apache.commons.math.linear.EigenDecomposition;
import org.apache.commons.math.linear.EigenDecompositionImpl;
import org.junit.Test;
import org.slf4j.helpers.NOPLogger;

import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

//RELEASE-STATUS: DIST

public class LeftSingularVectorParallelPowerMethodTest {

	@Test
	public void testArc() throws IOException {
		final ImmutableGraph graph = new ArrayListMutableGraph(2, new int[][] { { 0, 1 } }).immutableView();
		final ImmutableGraph transpose = new ArrayListMutableGraph(Transform.transpose(graph)).immutableView();

		for(double shift: new double[] { 0, -.1, -1 }) {
			final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(graph, transpose, NOPLogger.NOP_LOGGER);
			lsv.norm = Norm.L_INFINITY;
			lsv.shift = shift;
			lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(1E-5 / 100));

			assertEquals(0, lsv.rank[0], 1E-5);
			assertEquals(1, lsv.rank[1], 1E-5);
		}
	}

	@Test
	public void testSalsaConnectedM() throws IOException {
		final ImmutableGraph graph = new ArrayListMutableGraph(6, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 4 }, { 3, 1 }, { 3, 5 }, { 4, 0 }, { 4, 3 }, { 5, 3 }, { 5, 4 } }).immutableView();
		final ImmutableGraph transpose = new ArrayListMutableGraph(Transform.transpose(graph)).immutableView();
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(double shift: new double[] { 0, -.1, -1 }) {
					final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(graph, transpose, NOPLogger.NOP_LOGGER);
					lsv.norm = norm;
					lsv.salsa = true;
					lsv.shift = shift;
					lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 20));

					double[] expected = { 1, 2, 1, 2, 2, 1 };
					norm.normalize(expected, 1);
					for (int i = 0; i < expected.length; i++) assertEquals(expected[i], lsv.rank[i], threshold);
				}
			}
		}
	}

	@Test
	public void testSalsaNonconnectedM() throws IOException {
		final ImmutableGraph graph = new ArrayListMutableGraph(6, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 4 }, { 3, 1 }, { 3, 5 }, { 4, 0 }, { 4, 3 }, { 5, 3 } }).immutableView();
		final ImmutableGraph transpose = new ArrayListMutableGraph(Transform.transpose(graph)).immutableView();

		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(double shift: new double[] { 0, -.1, -1 }) {
					final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(graph, transpose, NOPLogger.NOP_LOGGER);
					lsv.norm = norm;
					lsv.salsa = true;
					lsv.shift = shift;
					lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));

					double[] indegree = { 1. / 3, 2. / 5, 1. / 5, 2. / 3, 1. / 5, 1. / 5 };
					double[] ccSize = { 2, 4, 4, 2, 4, 4 };
					double[] expected = new double[indegree.length];
					for (int i = 0; i < indegree.length; i++) expected[i] = indegree[i] * ccSize[i] / 6.0;
					lsv.norm.normalize(expected, 1);
					for (int i = 0; i < indegree.length; i++) assertEquals(expected[i] , lsv.rank[i], threshold);
				}
			}
		}
	}

	@Test
	public void testCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int size: new int[] { 100, 1000, 10000 }) {
					final ImmutableGraph bidirectionalCycle = ArrayListMutableGraph.newBidirectionalCycle(size).immutableView();
					final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(bidirectionalCycle, bidirectionalCycle, NOPLogger.NOP_LOGGER);
					lsv.norm = norm;
					lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
					final double[] expected = new double[size];
					Arrays.fill(expected, 1);
					norm.normalize(expected, 1);
					for(int i = lsv.graph.numNodes(); i-- != 0;) assertEquals(expected[i], lsv.rank[i], threshold);
				}
			}
		}
	}

	@Test
	public void testClique() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int size: new int[] { 10, 100, 1000 }) {
					final ImmutableGraph clique = ArrayListMutableGraph.newCompleteGraph(size, false).immutableView();
					final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(clique, clique, NOPLogger.NOP_LOGGER);
					lsv.norm = norm;
					lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
					final double[] expected = new double[size];
					Arrays.fill(expected, 1);
					lsv.norm.normalize(expected, 1);
					for(int i = lsv.graph.numNodes(); i-- != 0;) assertEquals(expected[i], lsv.rank[i], threshold);
				}
			}
		}
	}

	@Test
	public void testRandomSymmetric() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int size: new int[] { 10, 100 }) {
					for(double shift: new double[] { 0, -.1, -1 }) {
						// TODO refactor when symmetrize will return a copiable graph
						final ImmutableGraph graph = new ArrayListMutableGraph(Transform.symmetrize(new ErdosRenyiGraph(size, .3, 0, false))).immutableView();
						final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(graph, graph, NOPLogger.NOP_LOGGER);
						lsv.norm = norm;
						lsv.shift = shift;
						lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));

						Array2DRowRealMatrix m = new Array2DRowRealMatrix(graph.numNodes(), graph.numNodes());
						for(NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext();) {
							final int curr = nodeIterator.nextInt();
							LazyIntIterator successors = nodeIterator.successors();
							for(int s; (s = successors.nextInt()) != -1;) m.setEntry(curr, s, 1);
						}
						EigenDecomposition s = new EigenDecompositionImpl(m, 0);
						double[] qrDecomp = s.getEigenvector(0).toArray();
						qrDecomp = norm.normalize(qrDecomp, 1);
						for (int i = 0; i < graph.numNodes(); i++) qrDecomp[i] = Math.abs(qrDecomp[i]);

						for(int i = lsv.graph.numNodes(); i-- != 0;) assertEquals(qrDecomp[i], lsv.rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testRandom() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int size: new int[] { 10, 100 }) {
					for(double shift: new double[] { 0, -.1, -1 }) {
						// TODO refactor when symmetrize will return a copiable graph
						final ImmutableGraph graph = new ArrayListMutableGraph(new ErdosRenyiGraph(size, .3, 0, false)).immutableView();
						final ImmutableGraph transpose = new ArrayListMutableGraph(Transform.transpose(graph)).immutableView();
						final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(graph, transpose, NOPLogger.NOP_LOGGER);
						lsv.norm = norm;
						lsv.shift = shift;
						lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));

						Array2DRowRealMatrix m = new Array2DRowRealMatrix(graph.numNodes(), graph.numNodes());
						for(NodeIterator nodeIterator = graph.nodeIterator(); nodeIterator.hasNext();) {
							final int curr = nodeIterator.nextInt();
							LazyIntIterator successors = nodeIterator.successors();
							for(int s; (s = successors.nextInt()) != -1;) m.setEntry(curr, s, 1);
						}

						EigenDecomposition s = new EigenDecompositionImpl(m.transpose().multiply(m), 0);
						double[] qrDecomp = s.getEigenvector(0).toArray();
						qrDecomp = norm.normalize(qrDecomp, 1);
						for (int i = 0; i < graph.numNodes(); i++) qrDecomp[i] = Math.abs(qrDecomp[i]);

						for(int i = lsv.graph.numNodes(); i-- != 0;) assertEquals(qrDecomp[i], lsv.rank[i], threshold);
					}
				}
			}
		}
	}


	@Test
	public void testCliqueNobridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-10; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int p: new int[] { 10, 50, 100 }) {
					for(int k: new int[] { 10, 50, 100 }) {
						for(double shift: new double[] { 0, -.1, -1 }) {
							ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
							for(int i = 0; i < k; i++)
								for(int j = 0; j < k; j++) {
									if (i != j) mg.addArc(i, j);
								}
							for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
							ImmutableGraph g = mg.immutableView();
							ImmutableGraph gt = Transform.transpose(g);

							LeftSingularVectorParallelPowerMethod leftSingular = new LeftSingularVectorParallelPowerMethod(g, gt, NOPLogger.NOP_LOGGER);
							leftSingular.norm = norm;
							leftSingular.shift = shift;
							leftSingular.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] rank = leftSingular.rank;

							double ratio = rank[0];
							for(int i = k - 1; i-- != 0;) assertEquals(ratio * 1, rank[i], threshold);
							for(int d = 0; d < p; d++) assertEquals(ratio * 0, rank[k + d], threshold);

							leftSingular.salsa = true;
							leftSingular.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] expected = new double[rank.length];
							Arrays.fill(expected, 1);
							norm.normalize(expected, 1);
							assertArrayEquals(expected, rank, threshold);
						}
					}
				}
			}
		}
	}


	@Test
	public void testCliqueForwardbridgeCycle() throws IOException {
		for(int p: new int[] { 10, 50, 100 }) {
			for(int k: new int[] { 10, 50, 100 }) {
				ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++) {
						if (i != j) mg.addArc(i, j);
					}
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k - 1, k);
				ImmutableGraph g = mg.immutableView();
				ImmutableGraph gt = Transform.transpose(g);

				Array2DRowRealMatrix m = new Array2DRowRealMatrix(g.numNodes(), g.numNodes());
				for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
					final int curr = nodeIterator.nextInt();
					LazyIntIterator successors = nodeIterator.successors();
					for(int s; (s = successors.nextInt()) != -1;) m.setEntry(curr, s, 1);
				}
				double lambda = new EigenDecompositionImpl(m.multiply(m.transpose()), 0).getRealEigenvalue(0);

				for (double threshold = 1E-1; threshold > 1E-10; threshold /= 10) {
					for(Norm norm : Norm.values()) {
						for(double shift: new double[] { 0, -.1, -1 }) {

							LeftSingularVectorParallelPowerMethod leftSingular = new LeftSingularVectorParallelPowerMethod(g, gt, NOPLogger.NOP_LOGGER);
							leftSingular.norm = norm;
							leftSingular.shift = shift;
							leftSingular.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] rank = leftSingular.rank;

							double[] expected = new double[rank.length];
							for(int i = k - 1; i-- != 0;) expected[i] = lambda - k + 1;
							expected[k - 1] = (k - 1) * (k - 2);
							expected[k] = (k - 1) * (k - 1) + lambda * (lambda + 2 * k - 2 - k * k);
							norm.normalize(expected, 1);

							assertArrayEquals(expected, rank, threshold);

							leftSingular.salsa = true;
							leftSingular.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							for(int i = k; i-- != 0;) expected[i] = (k + 1) * (k - 1) / (2. + k * (k - 1));
							expected[k] = (k + 1) * 2 / (2. + k * (k - 1));
							for(int d = 1; d < p; d++) expected[k + d] = 1;

							norm.normalize(expected, 1);
							assertArrayEquals(expected, rank, threshold);
						}
					}
				}
			}
		}
	}

	@Test
	public void testCliqueBackbridgeCycle() throws IOException {
		for(int p: new int[] { 10, 50, 100 }) {
			for(int k: new int[] { 10, 50, 100 }) {
				ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++) {
						if (i != j) mg.addArc(i, j);
					}
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k, k - 1);
				ImmutableGraph g = mg.immutableView();
				ImmutableGraph gt = Transform.transpose(g);

				Array2DRowRealMatrix m = new Array2DRowRealMatrix(g.numNodes(), g.numNodes());
				for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
					final int curr = nodeIterator.nextInt();
					LazyIntIterator successors = nodeIterator.successors();
					for(int s; (s = successors.nextInt()) != -1;) m.setEntry(curr, s, 1);
				}
				double lambda = new EigenDecompositionImpl(m.multiply(m.transpose()), 0).getRealEigenvalue(0);

				for (double threshold = 1E-1; threshold > 1E-10; threshold /= 10) {
					for(Norm norm : Norm.values()) {
						for(double shift: new double[] { 0, -.1, -1 }) {

							LeftSingularVectorParallelPowerMethod leftSingular = new LeftSingularVectorParallelPowerMethod(g, gt, NOPLogger.NOP_LOGGER);
							leftSingular.norm = norm;
							leftSingular.shift = shift;
							leftSingular.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] rank = leftSingular.rank;

							double[] expected = new double[rank.length];
							for(int i = k - 1; i-- != 0;) expected[i] = lambda * lambda - lambda * (k + 1) + k - 1;
							expected[k - 1] = (k - 1) * (k - 2) * (lambda - 1);
							expected[k + 1] = (k - 1) * (k - 2);
							norm.normalize(expected, 1);

							assertArrayEquals(expected, rank, threshold);

							leftSingular.salsa = true;
							leftSingular.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							for(int i = k - 1; i-- != 0;) expected[i] = (k + 1) * (k - 1) / (2. + k * (k - 1));
							expected[k - 1] = (k + 1) * k / (2. + k * (k - 1));
							expected[k] = 1;
							expected[k + 1] = (k + 1) / (2. + k * (k - 1));
							for(int d = 2; d < p; d++) expected[k + d] = 1;

							norm.normalize(expected, 1);
							assertArrayEquals(expected, rank, threshold);
						}
					}
				}
			}
		}
	}

	@Test
	public void testCliqueBibridgeCycle() throws IOException {
		for(int p: new int[] { 10, 50, 100 }) {
			for(int k: new int[] { 10, 50, 100 }) {
				ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++) {
						if (i != j) mg.addArc(i, j);
					}
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k, k - 1);
				mg.addArc(k - 1, k);
				ImmutableGraph g = mg.immutableView();
				ImmutableGraph gt = Transform.transpose(g);

				Array2DRowRealMatrix m = new Array2DRowRealMatrix(g.numNodes(), g.numNodes());
				for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
					final int curr = nodeIterator.nextInt();
					LazyIntIterator successors = nodeIterator.successors();
					for(int s; (s = successors.nextInt()) != -1;) m.setEntry(curr, s, 1);
				}
				double lambda = new EigenDecompositionImpl(m.multiply(m.transpose()), 0).getRealEigenvalue(0);

				for (double threshold = 1E-1; threshold > 1E-10; threshold /= 10) {
					for(Norm norm : Norm.values()) {
						for(double shift: new double[] { 0, -.1, -1 }) {

							LeftSingularVectorParallelPowerMethod leftSingular = new LeftSingularVectorParallelPowerMethod(g, gt, NOPLogger.NOP_LOGGER);
							leftSingular.norm = norm;
							leftSingular.shift = shift;
							leftSingular.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] rank = leftSingular.rank;

							double[] expected = new double[rank.length];
							for(int i = k - 1; i-- != 0;) expected[i] = lambda * lambda - lambda * (k + 1) + k - 1;
							expected[k - 1] = (k - 1) * (k - 2) * (lambda - 1);
							expected[k] = lambda * lambda * lambda - lambda * lambda * (k * k - 2 * k + 4) + lambda * (3 * k * k - 7 * k + 6) - (k - 1) * (k - 1);
							expected[k + 1] = (k - 1) * (k - 2);
							norm.normalize(expected, 1);

							assertArrayEquals(expected, rank, threshold);

							leftSingular.salsa = true;
							leftSingular.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							for(int i = k - 1; i-- != 0;) expected[i] = (k + 2) * (k - 1) / (4. + k * (k - 1));
							expected[k - 1] = (k + 2) * k / (4. + k * (k - 1));
							expected[k] = (k + 2) * 2 / (4. + k * (k - 1));
							expected[k + 1] = (k + 2) / (4. + k * (k - 1));
							for(int d = 2; d < p; d++) expected[k + d] = 1;

							norm.normalize(expected, 1);
							assertArrayEquals(expected, rank, threshold);
						}
					}
				}
			}
		}
	}
}

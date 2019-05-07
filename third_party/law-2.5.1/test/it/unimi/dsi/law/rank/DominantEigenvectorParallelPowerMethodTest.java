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



//RELEASE-STATUS: DIST

public class DominantEigenvectorParallelPowerMethodTest {

	@Test
	public void testCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int size: new int[] { 100, 1000, 10000 }) {
					for(double shift: new double[] { 0, -.1, -1 }) {
						final DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(ArrayListMutableGraph.newDirectedCycle(size).immutableView(), NOPLogger.NOP_LOGGER);
						dominant.norm = norm;
						dominant.shift = shift;
						dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
						assertEquals(1, dominant.lambda, threshold);
						final double[] expected = new double[size];
						Arrays.fill(expected, 1);
						dominant.norm.normalize(expected, 1);
						for(int i = dominant.graph.numNodes(); i-- != 0;) assertEquals(expected[i], dominant.rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testClique() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int size: new int[] { 10, 100, 1000 }) {
					for(double shift: new double[] { 0, -.1, -1 }) {
						final DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(ArrayListMutableGraph.newCompleteGraph(size, false).immutableView(), NOPLogger.NOP_LOGGER);
						dominant.norm = norm;
						dominant.shift = shift;
						dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
						assertEquals(size - 1, dominant.lambda, threshold);
						final double[] expected = new double[size];
						Arrays.fill(expected, 1);
						dominant.norm.normalize(expected, 1);
						for(int i = dominant.graph.numNodes(); i-- != 0;) assertEquals(expected[i], dominant.rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueBibridgeCycle() throws IOException {
		for (double threshold = 1E-4; threshold > 1E-10; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int p: new int[] { 10, 50, 100 }) {
					for(int k: new int[] { 10, 50, 100 }) {
						for(double shift: new double[] { 0, -.1, -1 }) {
							ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
							for(int i = 0; i < k; i++)
								for(int j = 0; j < k; j++) {
									if (i != j) mg.addArc(i, j);
								}
							// Note the transposition
							for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
							mg.addArc(k - 1, k);
							mg.addArc(k, k - 1);
							ImmutableGraph g = mg.immutableView();
							Array2DRowRealMatrix m = new Array2DRowRealMatrix(p + k, p + k);
							for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
								final int curr = nodeIterator.nextInt();
								LazyIntIterator successors = nodeIterator.successors();
								for(int s; (s = successors.nextInt()) != -1;) m.setEntry(curr, s, 1);
							}

							DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
							dominant.norm = norm;
							dominant.shift = shift;
							dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] rank = dominant.rank;
							double lambda = dominant.lambda;

							double ratio = rank[k] / (lambda + 1);
							assertEquals(ratio * (1 + 1 / (lambda + 1 - k)), rank[k - 1], threshold);
							for(int i = k - 1; i-- != 0;) assertEquals(ratio / (lambda + 1 - k), rank[i], threshold);
							for(int d = 1; d < p - 1; d++) assertEquals(ratio * (lambda + 1) / pow(lambda, d), rank[k + d], threshold);


							DominantEigenvectorParallelPowerMethod markovian = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
							markovian.markovian = true;
							markovian.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							rank = markovian.rank;
							Norm.L_1.normalize(rank, 1);
							assertEquals(1, markovian.lambda, threshold);
							assertEquals(2. / (k * (k - 1) + p + 2), rank[k], threshold);
							assertEquals(k / (k * (k - 1) + p + 2.), rank[k - 1], threshold);
							for(int i = k - 1; i-- != 0;) assertEquals((k - 1) / (k * (k - 1) + p + 2.), rank[i], threshold);
							for(int d = 1; d < p; d++) assertEquals(1 / (k * (k - 1) + p + 2.), rank[k + d], threshold);

							// Test lambda on symmetric
							for(int i = 0; i < p; i++) {
								mg.addArc(k + i, k + (i + 1) % p);
								m.setEntry(k + i, k + (i + 1) % p, 1);
							}
							EigenDecomposition s = new EigenDecompositionImpl(m, 0);
							dominant = new DominantEigenvectorParallelPowerMethod(mg.immutableView(), NOPLogger.NOP_LOGGER);
							dominant.norm = norm;
							dominant.shift = shift;
							dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							assertEquals(s.getRealEigenvalue(0), dominant.lambda, threshold);
						}
					}
				}
			}
		}
	}

	@Test
	public void testCliqueBackbridgeCycle() throws IOException {
		for (double threshold = 1E-4; threshold > 1E-10; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int p: new int[] { 10, 50, 100 }) {
					for(int k: new int[] { 10, 50, 100 }) {
						for(double shift: new double[] { 0, -.1, -1 }) {
							ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
							for(int i = 0; i < k; i++)
								for(int j = 0; j < k; j++) {
									if (i != j) mg.addArc(i, j);
								}
							// Note the transposition
							for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
							mg.addArc(k - 1, k);
							ImmutableGraph g = mg.immutableView();
							Array2DRowRealMatrix m = new Array2DRowRealMatrix(p + k, p + k);
							for(NodeIterator nodeIterator = g.nodeIterator(); nodeIterator.hasNext();) {
								final int curr = nodeIterator.nextInt();
								LazyIntIterator successors = nodeIterator.successors();
								for(int s; (s = successors.nextInt()) != -1;) m.setEntry(curr, s, 1);
							}

							DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
							dominant.norm = norm;
							dominant.shift = shift;
							dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] rank = dominant.rank;

							double ratio = rank[0] * k * (k - 2) / ((k - 1) * (k - 1));

							for(int i = k - 2; i-- != 0;) assertEquals(ratio * (k - 1) * (k - 1) / (k * (k - 2)) , rank[i], threshold);
							for(int d = 0; d < p; d++) assertEquals(0, rank[k + d], threshold);


							DominantEigenvectorParallelPowerMethod markovian = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
							markovian.markovian = true;
							markovian.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							rank = markovian.rank;
							Norm.L_1.normalize(rank, 1);
							assertEquals(1, markovian.lambda, threshold);
							ratio = rank[0] * k * (k - 2) / ((k - 1) * (k - 1));

							for(int i = k - 2; i-- != 0;) assertEquals(ratio * (k - 1) * (k - 1) / (k * (k - 2)) , rank[i], threshold);
							for(int d = 0; d < p; d++) assertEquals(0, rank[k + d], threshold);
						}
					}
				}
			}
		}
	}

	@Test
	public void testCliqueForwardbridgeCycle() throws IOException {
		for (double threshold = 1E-4; threshold > 1E-10; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int p: new int[] { 10, 50, 100 }) {
					for(int k: new int[] { 10, 50, 100 }) {
						for(double shift: new double[] { 0, -.1, -1 }) {
							ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
							for(int i = 0; i < k; i++)
								for(int j = 0; j < k; j++) {
									if (i != j) mg.addArc(i, j);
								}
							// Note the transposition
							for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
							mg.addArc(k, k - 1);
							ImmutableGraph g = mg.immutableView();

							DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
							dominant.norm = norm;
							dominant.shift = shift;
							dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] rank = dominant.rank;

							double ratio = rank[0];
							for(int i = k - 1; i-- != 0;) assertEquals(ratio * 1, rank[i], threshold);
							for(int d = 0; d < p; d++) assertEquals(ratio * pow(k - 1 , p - d - 1) / (pow(k - 1, p) - 1), rank[k + d], threshold);
						}
					}
				}
			}
		}
	}

	@Test
	public void testCliqueNobridgeCycle() throws IOException {
		for (double threshold = 1E-4; threshold > 1E-10; threshold /= 10) {
			for(Norm norm : Norm.values()) {
				for(int p: new int[] { 10, 50, 100 }) {
					for(int k: new int[] { 10, 50, 100 }) {
						for(double shift: new double[] { 0, -.1, -1 }) {
							ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
							for(int i = 0; i < k; i++)
								for(int j = 0; j < k; j++) {
									if (i != j) mg.addArc(i, j);
								}
							// Note the transposition
							for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
							ImmutableGraph g = mg.immutableView();

							DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
							dominant.norm = norm;
							dominant.shift = shift;
							dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
							double[] rank = dominant.rank;

							double ratio = rank[0];
							for(int i = k - 1; i-- != 0;) assertEquals(ratio * 1, rank[i], threshold);
							for(int d = 0; d < p; d++) assertEquals(ratio * 0, rank[k + d], threshold);

						}
					}
				}
			}
		}
	}
}

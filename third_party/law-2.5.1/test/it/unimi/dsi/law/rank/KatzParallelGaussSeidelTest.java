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

import org.junit.Test;
import org.slf4j.helpers.NOPLogger;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;



//RELEASE-STATUS: DIST

public class KatzParallelGaussSeidelTest {

	@Test
	public void testCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(double alpha: new double[] { .25, .50, .75 }) {
				for(int size: new int[] { 100, 1000, 10000 }) {
					final KatzParallelGaussSeidel katz = new KatzParallelGaussSeidel(ArrayListMutableGraph.newDirectedCycle(size).immutableView());
					katz.alpha = alpha;
					katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
					for(int i = katz.graph.numNodes(); i-- != 0;) assertEquals(1 / (1 - katz.alpha), katz.rank[i], threshold);
				}
			}
		}
	}

	@Test
	public void testClique() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(double alpha: new double[] { .25, .50, .75 }) {
				for(int size: new int[] { 10, 100, 1000 }) {
					final KatzParallelGaussSeidel katz = new KatzParallelGaussSeidel(ArrayListMutableGraph.newCompleteGraph(size, false).immutableView());
					katz.alpha = alpha / (size - 1);
					katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
					for(int i = katz.graph.numNodes(); i-- != 0;) assertEquals(1 / (1 - katz.alpha * (size - 1)), katz.rank[i], threshold);

					final KatzParallelGaussSeidel katz2 = new KatzParallelGaussSeidel(ArrayListMutableGraph.newCompleteGraph(size, true).immutableView());
					katz2.alpha = alpha / size;
					katz2.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
					for(int i = katz.graph.numNodes(); i-- != 0;) assertEquals(1 / (1 - katz2.alpha * size), katz2.rank[i], threshold);
				}
			}
		}
	}

	@Test
	public void testCliqueBibridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(int p: new int[] { 10, 50, 100 }) {
				for(int k: new int[] { 10, 50, 100 }) {
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

					// Compute dominant eigenvalue
					DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
					dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
					double lambda = dominant.lambda;

					PowerSeries w = new PowerSeries(g);
					w.alpha = .8 / lambda;
					w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(double alpha: new double[] { .25, .50, .75 }) {
						// Compute index
						KatzParallelGaussSeidel katz = new KatzParallelGaussSeidel(g);
						katz.alpha = alpha / lambda;
						katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
						double[] rank = katz.rank;
						double[] expected = new double[rank.length];

						double r = expected[k - 1] = rank[k - 1];
						for(int i = k - 1; i-- != 0;) expected[i] = (1 + katz.alpha * r) / (1 - katz.alpha * (k - 2));
						for(int d = 0; d < p; d++) expected[k + d] = 1 / (1 - katz.alpha) + r * pow(katz.alpha, d + 1) / (1 - pow(katz.alpha, p));

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);

						katz.normVector(w.previousRank, w.maxRatio);
						katz.alpha = alpha / lambda;
						katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
						rank = katz.rank;

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueBackbridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(int p: new int[] { 10, 50, 100 }) {
				for(int k: new int[] { 10, 50, 100 }) {
					ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
					for(int i = 0; i < k; i++)
						for(int j = 0; j < k; j++) {
							if (i != j) mg.addArc(i, j);
						}
					// Note the transposition
					for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					mg.addArc(k - 1, k);
					ImmutableGraph g = mg.immutableView();

					// Compute dominant eigenvalue
					DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
					dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
					double lambda = dominant.lambda;

					PowerSeries w = new PowerSeries(g);
					w.alpha = .8 / lambda;
					w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(double alpha: new double[] { .25, .50, .75 }) {
						// Compute index
						KatzParallelGaussSeidel katz = new KatzParallelGaussSeidel(g);
						katz.alpha = alpha / lambda;
						katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
						double[] rank = katz.rank;
						double[] expected = new double[rank.length];

						for (int i = k - 1; i-- != 0;)
							expected[i] = -1 / ((1 - katz.alpha) * (katz.alpha * katz.alpha * (k - 1) + katz.alpha * (k - 2) - 1));
						expected[k - 1] = (katz.alpha * katz.alpha * (k - 1) - katz.alpha - 1) / ((1 - katz.alpha) * (katz.alpha * katz.alpha * (k - 1) + katz.alpha * (k - 2) - 1));
						for (int d = 0; d < p; d++)
							expected[k + d] = 1 / (1 - katz.alpha);

						for(int i = 0; i < rank.length; i++)
							assertEquals(expected[i], rank[i], threshold);

						katz.normVector(w.previousRank, w.maxRatio);
						katz.alpha = alpha / lambda;
						katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
						rank = katz.rank;

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueForwardbridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(int p: new int[] { 10, 50, 100 }) {
				for(int k: new int[] { 10, 50, 100 }) {
					ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
					for(int i = 0; i < k; i++)
						for(int j = 0; j < k; j++) {
							if (i != j) mg.addArc(i, j);
						}
					// Note the transposition
					for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					mg.addArc(k, k - 1);
					ImmutableGraph g = mg.immutableView();

					// Compute dominant eigenvalue
					DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
					dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
					double lambda = dominant.lambda;

					PowerSeries w = new PowerSeries(g);
					w.alpha = .8 / lambda;
					w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(double alpha: new double[] { .25, .50, .75 }) {
						// Compute index
						KatzParallelGaussSeidel katz = new KatzParallelGaussSeidel(g);
						katz.alpha = alpha / lambda;
						katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
						double[] rank = katz.rank;
						double[] expected = new double[rank.length];

						for (int i = k; i-- != 0;)
							expected[i] = 1 / (1 - (k - 1) * katz.alpha);
						for (int d = 0; d < p; d++)
							expected[k + d] = 1 / (1 - katz.alpha) + pow(katz.alpha, d + 1) / ((1 - (k - 1) * katz.alpha) * (1 - pow(katz.alpha, p)));

						for(int i = 0; i < rank.length; i++)
							assertEquals(expected[i], rank[i], threshold);

						katz.normVector(w.previousRank, w.maxRatio);
						katz.alpha = alpha / lambda;
						katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
						rank = katz.rank;

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueNobridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(int p: new int[] { 10, 50, 100 }) {
				for(int k: new int[] { 10, 50, 100 }) {
					ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
					for(int i = 0; i < k; i++)
						for(int j = 0; j < k; j++) {
							if (i != j) mg.addArc(i, j);
						}
					// Note the transposition
					for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					ImmutableGraph g = mg.immutableView();

					// Compute dominant eigenvalue
					DominantEigenvectorParallelPowerMethod dominant = new DominantEigenvectorParallelPowerMethod(g, NOPLogger.NOP_LOGGER);
					dominant.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
					double lambda = dominant.lambda;

					PowerSeries w = new PowerSeries(g);
					w.alpha = .8 / lambda;
					w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(double alpha: new double[] { .25, .50, .75 }) {
						// Compute index
						KatzParallelGaussSeidel katz = new KatzParallelGaussSeidel(g);
						katz.alpha = alpha / lambda;
						katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
						double[] rank = katz.rank;
						double[] expected = new double[rank.length];

						for (int i = k; i-- != 0;)
							expected[i] = 1 / (1 - (k - 1) * katz.alpha);
						for (int d = 0; d < p; d++)
							expected[k + d] = 1 / (1 - katz.alpha);

						for(int i = 0; i < rank.length; i++)
							assertEquals(expected[i], rank[i], threshold);

						katz.normVector(w.previousRank, w.maxRatio);
						katz.alpha = alpha / lambda;
						katz.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));
						rank = katz.rank;

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);
					}
				}
			}
		}
	}

}

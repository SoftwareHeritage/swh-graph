package it.unimi.dsi.law.big.rank;

/*
 *  Copyright (C) 2006-2019 Paolo Boldi, Roberto Posenato, Massimo Santini and Sebastiano Vigna
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

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.law.TestUtil;
import it.unimi.dsi.law.rank.PowerSeries;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;



//RELEASE-STATUS: DIST

public class PageRankParallelGaussSeidelTest {
	final static String GRAPH_NAME = "test50-.6-7-3-2-10-graph";
	static double[][] exactResult;
	static double[][] preference;
	static long n;
	static String baseNameGraph;
	static String baseNamePreference;
	static ImmutableGraph g;

	@BeforeClass
	public static void setUp() throws Exception {
		baseNameGraph = TestUtil.getTestFile(it.unimi.dsi.law.rank.PageRankParallelGaussSeidelTest.class, GRAPH_NAME, false);
		baseNamePreference = baseNameGraph + "-preferenceVector";

		g = ImmutableGraph.load(baseNameGraph + "T"); // I need the transposed graph!
		n = g.numNodes();
		exactResult = DoubleBigArrays.newBigArray(n);
		preference = DoubleBigArrays.newBigArray(n);
	}

	@Test
	public void testRank() throws Exception {
		System.out.println("rank without preference vector");
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = null;
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-uniform-w.out", exactResult);
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testRankWithUniformPreferenceVector() throws Exception {
		System.out.println("rank with uniform preference vector");
		BinIO.loadDoubles(baseNamePreference + "-uniform.bin", preference);
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = DoubleBigArrayBigList.wrap(preference);
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-uniform-w.out", exactResult);
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testRankWithAlternatePreferenceVector() throws Exception {
		System.out.println("rank with uniform alternate vector");
		BinIO.loadDoubles(baseNamePreference + "-alternate.bin", preference);
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = DoubleBigArrayBigList.wrap(preference);
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-alternate-w.out", exactResult);
			System.out.println(Arrays.toString(exactResult));
			System.out.println(Arrays.toString(pr.rank));
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testRankWith1stHalfPreferenceVector() throws Exception {
		System.out.println("rank with uniform 1stHalf vector");
		BinIO.loadDoubles(baseNamePreference + "-1stHalf.bin", preference);
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = DoubleBigArrayBigList.wrap(preference);
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-1stHalf-w.out", exactResult);
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testRankWith2ndHalfPreferenceVector() throws Exception {
		System.out.println("rank with uniform 2ndHalf vector");
		BinIO.loadDoubles(baseNamePreference + "-2ndHalf.bin", preference);
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = DoubleBigArrayBigList.wrap(preference);
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-2ndHalf-w.out", exactResult);
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testStrongRankWithUniformPreferenceVector() throws Exception {
		System.out.println("Strong rank with uniform preference vector");
		BinIO.loadDoubles(baseNamePreference + "-uniform.bin", preference);
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = DoubleBigArrayBigList.wrap(preference);
		pr.stronglyPreferential = true;
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-uniform-s.out", exactResult);
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testStrongRankWithAlternatePreferenceVector() throws Exception {
		System.out.println("Strong rank with uniform alternate vector");
		BinIO.loadDoubles(baseNamePreference + "-alternate.bin", preference);
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = DoubleBigArrayBigList.wrap(preference);
		pr.stronglyPreferential = true;
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-alternate-s.out", exactResult);
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testStrongRankWith1stHalfPreferenceVector() throws Exception {
		System.out.println("Strong rank with uniform 1stHalf vector");
		BinIO.loadDoubles(baseNamePreference + "-1stHalf.bin", preference);
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = DoubleBigArrayBigList.wrap(preference);
		pr.stronglyPreferential = true;
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-1stHalf-s.out", exactResult);
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testStrongRankWith2ndHalfPreferenceVector() throws Exception {
		System.out.println("Strong rank with uniform 2ndHalf vector");
		BinIO.loadDoubles(baseNamePreference + "-2ndHalf.bin", preference);
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = DoubleBigArrayBigList.wrap(preference);
		pr.stronglyPreferential = true;
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			TextIO.loadDoubles(baseNamePreference + "-2ndHalf-s.out", exactResult);
			assertEquals("Too much different!", 0.0, Norm.L_1.compute(pr.rank, exactResult), threshold);
		}
	}

	@Test
	public void testCliqueBibridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(final int p: new int[] { 10, 50, 100 }) {
				for(final int k: new int[] { 10, 50, 100 }) {
					final ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
					for(int i = 0; i < k; i++)
						for(int j = 0; j < k; j++)
							if (i != j) mg.addArc(i, j);
					// Note the transposition
					for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					mg.addArc(k - 1, k);
					mg.addArc(k, k - 1);
					final it.unimi.dsi.webgraph.ImmutableGraph g = mg.immutableView();

					final PowerSeries w = new PowerSeries(g);
					w.markovian = true;
					w.alpha = .8;
					w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(final double alpha: new double[] { .25, .50, .75 }) {
						// Compute index
						final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(ImmutableGraph.wrap(g));
						pr.alpha = alpha;
						pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
						final double[] rank = pr.rank[0];
						final double[] expected = new double[k + p];
						final double r = rank[k - 1] * (k + p);

						expected[k - 1] = r;
						for(int i = k - 1; i-- != 0;) expected[i] = (k - 1) * (k - alpha * k + alpha * r) / (k * (k - 1 - alpha * (k - 2)));
						expected[k] =  2 + 2 * (alpha * r - k) / (k * (2 - pow(alpha, p)));
						for(int d = 1; d < p; d++) expected[k + d] = 1 + pow(alpha, d) * (alpha * r - k) / (k * (2 - pow(alpha, p)));
						for(int i = expected.length; i-- != 0;) expected[i] /= k + p;

						assertEquals(0, Norm.L_1.compute(expected, rank), threshold);

						pr.normVector(DoubleBigArrays.wrap(w.previousRank), w.maxRatio);
						pr.pseudoRank = true;
						pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueBackbridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(final int p: new int[] { 10, 50, 100 }) {
				for(final int k: new int[] { 10, 50, 100 }) {
					final ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
					for(int i = 0; i < k; i++)
						for(int j = 0; j < k; j++)
							if (i != j) mg.addArc(i, j);
					// Note the transposition
					for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					mg.addArc(k - 1, k);
					final it.unimi.dsi.webgraph.ImmutableGraph g = mg.immutableView();

					final PowerSeries w = new PowerSeries(g);
					w.markovian = true;
					w.alpha = .8;
					w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(final double alpha: new double[] { .25, .50, .75 }) {
						// Compute index
						final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(ImmutableGraph.wrap(g));
						pr.alpha = alpha;
						pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
						final double[] rank = pr.rank[0];
						final double[] expected = new double[k + p];

						for(int i = k - 1; i-- != 0;)
							expected[i] = (2 * (k - 1) - 2 * (k - 2) * alpha - alpha * alpha) / (2 * (1 - alpha) * (k - 1 + alpha)) -
								pow(alpha, p + 2) / (2 * (1 - alpha) * (k - 1 + alpha) * (2 - pow(alpha, p)));

						expected[k - 1] =  (2 * (k - 1) - (k - 3) * alpha - alpha * alpha * k) / (2 * (1 - alpha) * (k - 1 + alpha)) -
								pow(alpha, p + 1) * (k - 1 - alpha * (k - 2)) / (2 * (1 - alpha) * (k - 1 + alpha) * (2 - pow(alpha, p)));
						for(int d = 0; d < p; d++)
							expected[k + d] = 1 - pow(alpha, d + (d == 0? p : 0)) / (2 - pow(alpha, p));

						for(int i = expected.length; i-- != 0;) expected[i] /= k + p;

						assertEquals(0, Norm.L_1.compute(expected, rank), threshold);

						pr.normVector(DoubleBigArrays.wrap(w.previousRank), w.maxRatio);
						pr.pseudoRank = true;
						pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueForwardbridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(final int p: new int[] { 10, 50, 100 }) {
				for(final int k: new int[] { 10, 50, 100 }) {
					final ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
					for(int i = 0; i < k; i++)
						for(int j = 0; j < k; j++)
							if (i != j) mg.addArc(i, j);
					// Note the transposition
					for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					mg.addArc(k, k - 1);
					final it.unimi.dsi.webgraph.ImmutableGraph g = mg.immutableView();

					final PowerSeries w = new PowerSeries(g);
					w.markovian = true;
					w.alpha = .8;
					w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(final double alpha: new double[] { .25, .50, .75 }) {
						// Compute index
						final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(ImmutableGraph.wrap(g));
						pr.alpha = alpha;
						pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
						final double[] rank = pr.rank[0];
						final double[] expected = new double[k + p];
						for(int i = k - 1; i-- != 0;)
							expected[i] = (1 - alpha) * (alpha + k) * (k - 1) / ((k - alpha * alpha) * (k - 1) - alpha * k * (k - 2));

						expected[k - 1] = k * (1 - alpha) * (k - 1 + alpha) / ((k - alpha * alpha) * (k - 1) - alpha * k * (k - 2));
						for(int d = 0; d < p; d++)
							expected[k + d] = 1 + (pow(alpha, d + 1) * (1 - alpha) * (k - 1 + alpha)) / ((1 - pow(alpha, p)) * ((k - alpha * alpha) * (k - 1) - alpha * k * (k - 2)));

						for(int i = expected.length; i-- != 0;) expected[i] /= k + p;

						assertEquals(0, Norm.L_1.compute(expected, rank), threshold);

						pr.normVector(DoubleBigArrays.wrap(w.previousRank), w.maxRatio);
						pr.pseudoRank = true;
						pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);
					}
				}
			}
		}
	}

	@Test
	public void testCliqueNobridgeCycle() throws IOException {
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			for(final int p: new int[] { 10, 50, 100 }) {
				for(final int k: new int[] { 10, 50, 100 }) {
					final ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
					for(int i = 0; i < k; i++)
						for(int j = 0; j < k; j++)
							if (i != j) mg.addArc(i, j);
					// Note the transposition
					for(int i = 0; i < p; i++) mg.addArc(k + (i + 1) % p, k + i);
					final it.unimi.dsi.webgraph.ImmutableGraph g = mg.immutableView();

					final PowerSeries w = new PowerSeries(g);
					w.markovian = true;
					w.alpha = .8;
					w.stepUntil(PowerSeries.MAX_RATIO_STOPPING_CRITERION);

					for(final double alpha: new double[] { .25, .50, .75 }) {
						// Compute index
						final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(ImmutableGraph.wrap(g));
						pr.alpha = alpha;
						pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
						final double[] rank = pr.rank[0];
						final double[] expected = new double[k + p];
						for(int i = k + p; i-- != 0;)
							expected[i] = 1;
						for(int i = expected.length; i-- != 0;) expected[i] /= k + p;

						assertEquals(0, Norm.L_1.compute(expected, rank), threshold);

						pr.normVector(DoubleBigArrays.wrap(w.previousRank), w.maxRatio);
						pr.pseudoRank = true;
						pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold));

						for(int i = 0; i < rank.length; i++) assertEquals(expected[i], rank[i], threshold);
					}
				}
			}
		}
	}


	@Test
	@Ignore("Needs lots of RAM")
	public void testBig() throws Exception {
		final long n = 1L << 31;
		final double rank = 1. / n;

		final ImmutableGraph g = new ImmutableGraph() {
			@Override
			public long numNodes() {
				return n;
			}

			@Override
			public boolean randomAccess() {
				return true;
			}

			@Override
			public long outdegree(final long x) {
				return 1;
			}

			@Override
			public long[][] successorBigArray(final long x) {
				return new long[][] { { (x + 1) % n } };
			}

			@Override
			public ImmutableGraph copy() {
				return this;
			}
		};
		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(g);
		pr.preference = null;
		for (double threshold = 1E-1; threshold > 1E-12; threshold /= 10) {
			pr.stepUntil(new SpectralRanking.NormStoppingCriterion(threshold / 10));
			for(int i = 0; i < n; i++) assertEquals(DoubleBigArrays.get(pr.rank, i), rank, threshold);
		}
	}

}

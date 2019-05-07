package it.unimi.dsi.law.rank;

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

import static it.unimi.dsi.law.rank.SpectralRanking.DEFAULT_THRESHOLD;
import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.law.TestUtil;
import it.unimi.dsi.law.rank.SpectralRanking.StoppingCriterion;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.webgraph.ImmutableGraph;



//RELEASE-STATUS: DIST

public class PageRankWithDerivativesDeepTest {
	final static String GRAPH_NAME = "test10-.7-2-2-2-5-graph";
	// We lose several digits when passing to the derivatives.
	final static double TEST_THRESHOLD = DEFAULT_THRESHOLD * 30;
	static double[] exactResult;
	static double[] preference;
	static int n;
	static String baseNameGraph;
	static String baseNamePreference;
	static ImmutableGraph g;
	static StoppingCriterion stop;

	@BeforeClass
	public static void setUp() throws Exception {
		baseNameGraph = TestUtil.getTestFile(PageRankWithDerivativesDeepTest.class, GRAPH_NAME, false);
		baseNamePreference = baseNameGraph + "-preferenceVector";

		g = ImmutableGraph.load(baseNameGraph);
		n = g.numNodes();
		exactResult = new double[n];
		preference = new double[n];
		stop = new SpectralRanking.NormStoppingCriterion(DEFAULT_THRESHOLD);
	}

	@Test
	public void testRank() throws Exception {
		System.out.println("rank without preference vector");
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = null;
		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-uniform-w.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-uniform-wd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-uniform-wd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}

	@Test
	public void testRankWithUniformPreferenceVector() throws Exception {
		System.out.println("rank with uniform preference vector");
		BinIO.loadDoubles(baseNamePreference + "-uniform.bin", preference);
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = DoubleArrayList.wrap(preference);
		for (int i = 0; i < n; i++)
			assertEquals("Not really uniform! ", preference[i], 1.0 / n, DEFAULT_THRESHOLD);

		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-uniform-w.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-uniform-wd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-uniform-wd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}

	@Test
	public void testRankWithAlternatePreferenceVector() throws Exception {
		System.out.println("rank with uniform alternate vector");
		BinIO.loadDoubles(baseNamePreference + "-alternate.bin", preference);
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = DoubleArrayList.wrap(preference);
		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-alternate-w.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-alternate-wd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-alternate-wd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}

	@Test
	public void testRankWith1stHalfPreferenceVector() throws Exception {
		System.out.println("rank with uniform 1stHalf vector");
		BinIO.loadDoubles(baseNamePreference + "-1stHalf.bin", preference);
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = DoubleArrayList.wrap(preference);
		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-1stHalf-w.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-1stHalf-wd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-1stHalf-wd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}

	@Test
	public void testRankWith2ndHalfPreferenceVector() throws Exception {
		System.out.println("rank with uniform 2ndHalf vector");
		BinIO.loadDoubles(baseNamePreference + "-2ndHalf.bin", preference);
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = DoubleArrayList.wrap(preference);
		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-2ndHalf-w.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-2ndHalf-wd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-2ndHalf-wd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}

	@Test
	public void testStrongRankWithUniformPreferenceVector() throws Exception {
		System.out.println("Strong rank with uniform preference vector");
		BinIO.loadDoubles(baseNamePreference + "-uniform.bin", preference);
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = DoubleArrayList.wrap(preference);
		pr.stronglyPreferential = true;
		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-uniform-s.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-uniform-sd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-uniform-sd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}

	@Test
	public void testStrongRankWithAlternatePreferenceVector() throws Exception {
		System.out.println("Strong rank with uniform alternate vector");
		BinIO.loadDoubles(baseNamePreference + "-alternate.bin", preference);
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = DoubleArrayList.wrap(preference);
		pr.stronglyPreferential = true;
		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-alternate-s.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-alternate-sd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-alternate-sd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}

	@Test
	public void testStrongRankWith1stHalfPreferenceVector() throws Exception {
		System.out.println("Strong rank with uniform 1stHalf vector");
		BinIO.loadDoubles(baseNamePreference + "-1stHalf.bin", preference);
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = DoubleArrayList.wrap(preference);
		pr.stronglyPreferential = true;
		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-1stHalf-s.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-1stHalf-sd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-1stHalf-sd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}


	@Test
	public void testStrongRankWith2ndHalfPreferenceVector() throws Exception {
		System.out.println("Strong rank with uniform 2ndHalf vector");
		BinIO.loadDoubles(baseNamePreference + "-2ndHalf.bin", preference);
		PageRankPowerSeries pr = new PageRankPowerSeries(g);
		pr.order = new int[] { 1, 2 };
		pr.preference = DoubleArrayList.wrap(preference);
		pr.stronglyPreferential = true;
		pr.stepUntil(stop);

		TextIO.loadDoubles(baseNamePreference + "-2ndHalf-s.out", exactResult);
		double result[] = pr.rank;
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), DEFAULT_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-2ndHalf-sd1.out", exactResult);
		result = pr.derivative[0];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);

		TextIO.loadDoubles(baseNamePreference + "-2ndHalf-sd2.out", exactResult);
		result = pr.derivative[1];
		assertEquals("Too much different!", 0, Norm.L_1.compute(result, exactResult), TEST_THRESHOLD);
	}
}

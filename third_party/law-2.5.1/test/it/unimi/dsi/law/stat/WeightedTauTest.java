package it.unimi.dsi.law.stat;

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

import static it.unimi.dsi.law.stat.WeightedTau.HYPERBOLIC_WEIGHER;
import static it.unimi.dsi.law.stat.WeightedTau.LOGARITHMIC_WEIGHER;
import static it.unimi.dsi.law.stat.WeightedTau.QUADRATIC_WEIGHER;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.ints.Int2DoubleFunction;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

//RELEASE-STATUS: DIST

public class WeightedTauTest {
	private static final Int2DoubleFunction CONSTANT_WEIGHER = new WeightedTau.AbstractWeigher() {
		private static final long serialVersionUID = 1L;
		@Override
		public double get(int key) {
			return 1;
		}
	};
	private static final Int2DoubleFunction[] WEIGHER = new Int2DoubleFunction[] { CONSTANT_WEIGHER, HYPERBOLIC_WEIGHER, LOGARITHMIC_WEIGHER, QUADRATIC_WEIGHER };
	private final double[] ordered = { 0.0, 1.0, 2.0, 3.0, 4.0 };
	private final double[] reverse = { 4.0, 3.0, 2.0, 1.0, 0.0 };
	private final double[] reverseButOne = { 10.0, 9.0, 7.0, 8.0, 6.0 };
	private final double[] allOnes = { 1.0, 1.0, 1.0 };
	private final double[] allZeroes = { 0.0, 0.0, 0.0 };

	public static double compute(Int2DoubleFunction weigher, boolean multiplicative, double[] v0, double[] v1, int rank[]) {
		double concordances = 0, discordances = 0, u = 0, v = 0, tot = 0;
		final int length = v0.length;

		if (rank == null) {
			rank = Util.identity(length);
			DoubleArrays.radixSortIndirect(rank, v1, v0, true);
			IntArrays.reverse(rank);
			Util.invertPermutationInPlace(rank);
		}

		for (int i = 0; i < length; i++) {
			for (int j = i + 1; j < length; j++) {
				final double weight = multiplicative ? weigher.get(rank[i]) * weigher.get(rank[j]) : weigher.get(rank[i]) + weigher.get(rank[j]);
				tot += weight;
				if (v0[i] == v0[j]) u += weight;
				if (v1[i] == v1[j]) v += weight;
				if (v0[i] < v0[j] && v1[i] < v1[j] || v0[i] > v0[j] && v1[i] > v1[j]) concordances += weight;
				else if (v0[i] < v0[j] && v1[i] > v1[j] || v0[i] > v0[j] && v1[i] < v1[j]) discordances += weight;
			}
		}

		//System.err.println("u: " + u + " v: " + v + " tot:" + tot + " conc: " + concordances + " disc: " + discordances);
		return (concordances - discordances) / (Math.sqrt((tot - u) * (tot - v)));
	}

	@Test
	public void testComputeOrdered() {
		for (boolean multiplicative: new boolean[] { false, true}) {
			for (Int2DoubleFunction weigher : WEIGHER) {
				WeightedTau weightedTau = new WeightedTau(weigher, multiplicative);
				double expResult = compute(weigher, multiplicative, ordered, ordered, null); // (10.0 - 0.0) / 10.0;
				double result = weightedTau.compute(ordered, ordered, null);
				assertEquals(expResult, result, 1E-15);
			}
		}
	}

	@Test
	public void testComputeWithReverse() {
		for (boolean multiplicative: new boolean[] { false, true}) {
			for (Int2DoubleFunction weigher : WEIGHER) {
				WeightedTau weightedTau = new WeightedTau(weigher, multiplicative);
				double expResult = compute(weigher, multiplicative, ordered, reverse, null);// (0 - 10.0) / 10.0;
				double result = weightedTau.compute(ordered, reverse, null);
				assertEquals(expResult, result, 1E-15);

				expResult = compute(weigher, multiplicative, reverse, ordered, null);// (0 - 10.0) / 10.0;
				result = weightedTau.compute(reverse, ordered, null);
				assertEquals(expResult, result, 1E-15);
			}
		}
	}

	@Test
	public void testComputeWithReverseButOne() {
		for (boolean multiplicative: new boolean[] { false, true}) {
			for (Int2DoubleFunction weigher : WEIGHER) {
				WeightedTau weightedTau = new WeightedTau(weigher, multiplicative);
				double expResult = compute(weigher, multiplicative, ordered, reverseButOne, null);// (1.0 - 9.0) / 10.0;
				double result = weightedTau.compute(ordered, reverseButOne, null);
				assertEquals(expResult, result, 1E-15);

				expResult = compute(weigher, multiplicative, reverseButOne, ordered, null);// (1.0 - 9.0) / 10.0;
				result = weightedTau.compute(reverseButOne, ordered, null);
				assertEquals(expResult, result, 1E-15);
			}
		}
	}

	@Test
	public void testTies() {
		for (boolean multiplicative: new boolean[] { false, true}) {
			for (Int2DoubleFunction weigher : WEIGHER) {
				WeightedTau weightedTau = new WeightedTau(weigher, multiplicative);
				final double[] v0 = { 0.1, 0.1, 0.2 };
				final double[] v1 = { 0.4, 0.3, 0.3 };

				double expResult = compute(weigher, multiplicative, v0, v1, null);
				double result = weightedTau.compute(v0, v1, null);
				assertEquals(expResult, result, 1E-15);

				expResult = compute(weigher, multiplicative, v1, v0, null);
				result = weightedTau.compute(v1, v0, null);
				assertEquals(expResult, result, 1E-15);
			}
		}
	}

	@Test
	public void testRandom() {
		for (boolean multiplicative: new boolean[] { false, true}) {
			for (Int2DoubleFunction weigher : WEIGHER) {
				XoRoShiRo128PlusRandom XoRoShiRo128PlusRandom = new XoRoShiRo128PlusRandom(0);
				WeightedTau weightedTau = new WeightedTau(weigher, multiplicative);
				final double v0[] = new double[1000];
				final double v1[] = new double[1000];

				for (int i = v0.length; i-- != 0;) {
					v0[i] = XoRoShiRo128PlusRandom.nextDouble();
					v1[i] = XoRoShiRo128PlusRandom.nextDouble();
				}
				double expResult = compute(weigher, multiplicative, v0, v1, null);
				double result = weightedTau.compute(v0, v1, null);
				assertEquals(expResult, result, 1E-10);

				expResult = compute(weigher, multiplicative, v1, v0, null);
				result = weightedTau.compute(v1, v0, null);
				assertEquals(expResult, result, 1E-10);
			}
		}
	}

	@Test
	public void testRandomWithTies() {
		for (boolean multiplicative: new boolean[] { false, true}) {
			for (Int2DoubleFunction weigher : WEIGHER) {
				WeightedTau weightedTau = new WeightedTau(weigher, multiplicative);
				XoRoShiRo128PlusRandom XoRoShiRo128PlusRandom = new XoRoShiRo128PlusRandom(0);
				final double v0[] = new double[1000];
				final double v1[] = new double[1000];
				for (int i = v0.length; i-- != 0;) {
					v0[i] = XoRoShiRo128PlusRandom.nextInt(10);
					v1[i] = XoRoShiRo128PlusRandom.nextInt(10);
				}

				double expResult = compute(weigher, multiplicative, v0, v1, null);
				double result = weightedTau.compute(v0, v1, null);
				assertEquals(expResult, result, 1E-10);

				expResult = compute(weigher, multiplicative, v1, v0, null);
				result = weightedTau.compute(v1, v0, null);
				assertEquals(expResult, result, 1E-10);
			}
		}
	}

	@Test
	public void testRandomWithTiesAndRank() {
		for (boolean multiplicative: new boolean[] { false, true}) {
			for (Int2DoubleFunction weigher : WEIGHER) {
				XoRoShiRo128PlusRandom XoRoShiRo128PlusRandom = new XoRoShiRo128PlusRandom(0);
				WeightedTau weightedTau = new WeightedTau(weigher, multiplicative);
				final double v0[] = new double[1000];
				final double v1[] = new double[1000];
				for (int i = v0.length; i-- != 0;) {
					v0[i] = XoRoShiRo128PlusRandom.nextInt(10);
					v1[i] = XoRoShiRo128PlusRandom.nextInt(10);
				}
				final int[] rank = Util.identity(v0.length);
				IntArrays.shuffle(rank,  XoRoShiRo128PlusRandom);

				double expResult = compute(weigher, multiplicative, v0, v1, rank);
				double result = weightedTau.compute(v0, v1, rank);
				assertEquals(expResult, result, 1E-10);

				expResult = compute(weigher, multiplicative, v1, v0, rank);
				result = weightedTau.compute(v1, v0, rank);
				assertEquals(expResult, result, 1E-10);
			}
		}
	}


	@Test
	public void testAllTies() {
		for (Int2DoubleFunction weigher : WEIGHER) {
			assertEquals(1.0, new WeightedTau(weigher).compute(allOnes, allZeroes, null), 1E-15);
		}
	}

	@Test
	public void testInputType() throws IOException {
		File a = File.createTempFile(WeightedTauTest.class.getSimpleName(), "a");
		a.deleteOnExit();
		File b = File.createTempFile(WeightedTauTest.class.getSimpleName(), "b");
		b.deleteOnExit();
		for(boolean reverse: new boolean[] { true, false }) {
			for (Int2DoubleFunction weigher : WEIGHER) {
				WeightedTau weightedTau = new WeightedTau(weigher);
				BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, a);
				BinIO.storeInts(new int[] { 4, 3, 2, 1, 0 }, b);
				assertEquals(-1, weightedTau.computeInts(a.toString(), b.toString(), reverse), 1E-15);
				// TODO: main test
				assertEquals(-1, weightedTau.compute(a.toString(), Integer.class, b.toString(), Integer.class, reverse, Integer.MAX_VALUE), 1E-15);
				BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, b);
				assertEquals(1, weightedTau.computeInts(a.toString(), b.toString(), reverse), 1E-15);
				assertEquals(1, weightedTau.compute(a.toString(), Integer.class, b.toString(), Integer.class, reverse, Integer.MAX_VALUE), 1E-15);

				BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, a);
				BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
				assertEquals(-1, weightedTau.computeLongs(a.toString(), b.toString(), reverse), 1E-15);
				assertEquals(-1, weightedTau.compute(a.toString(), Long.class, b.toString(), Long.class, reverse, Integer.MAX_VALUE), 1E-15);
				BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
				assertEquals(1, weightedTau.computeLongs(a.toString(), b.toString(), reverse), 1E-15);
				assertEquals(1, weightedTau.compute(a.toString(), Long.class, b.toString(), Long.class, reverse, Integer.MAX_VALUE), 1E-15);

				BinIO.storeFloats(new float[] { 0, 1, 2, 3, 4 }, a);
				BinIO.storeFloats(new float[] { 4, 3, 2, 1, 0 }, b);
				assertEquals(-1, weightedTau.computeFloats(a.toString(), b.toString(), reverse), 1E-15);
				assertEquals(-1, weightedTau.compute(a.toString(), Float.class, b.toString(), Float.class, reverse, Integer.MAX_VALUE), 1E-15);
				BinIO.storeFloats(new float[] { 0, 1, 2, 3, 4 }, b);
				assertEquals(1, weightedTau.computeFloats(a.toString(), b.toString(), reverse), 1E-15);
				assertEquals(1, weightedTau.compute(a.toString(), Float.class, b.toString(), Float.class, reverse, Integer.MAX_VALUE), 1E-15);

				BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
				BinIO.storeDoubles(new double[] { 4, 3, 2, 1, 0 }, b);
				assertEquals(-1, weightedTau.computeDoubles(a.toString(), b.toString(), reverse), 1E-15);
				assertEquals(-1, weightedTau.compute(a.toString(), Double.class, b.toString(), Double.class, reverse, Integer.MAX_VALUE), 1E-15);
				BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, b);
				assertEquals(1, weightedTau.computeDoubles(a.toString(), b.toString(), reverse), 1E-15);
				assertEquals(1, weightedTau.compute(a.toString(), Double.class, b.toString(), Double.class, reverse, Integer.MAX_VALUE), 1E-15);

				BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, a);
				BinIO.storeDoubles(new double[] { 4, 3, 2, 1, 0 }, b);
				assertEquals(-1, weightedTau.compute(a.toString(), Integer.class, b.toString(), Double.class, reverse, Integer.MAX_VALUE), 1E-15);
				BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, b);
				assertEquals(1, weightedTau.compute(a.toString(), Integer.class, b.toString(), Double.class, reverse, Integer.MAX_VALUE), 1E-15);

				BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
				BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
				assertEquals(-1, weightedTau.compute(a.toString(), Double.class, b.toString(), Long.class, reverse, Integer.MAX_VALUE), 1E-15);
				BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
				assertEquals(1, weightedTau.compute(a.toString(), Double.class, b.toString(), Long.class, reverse, Integer.MAX_VALUE), 1E-15);

				TextIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
				BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
				assertEquals(-1, weightedTau.compute(a.toString(), String.class, b.toString(), Long.class, reverse, Integer.MAX_VALUE), 1E-15);
				BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
				assertEquals(1, weightedTau.compute(a.toString(), String.class, b.toString(), Long.class, reverse, Integer.MAX_VALUE), 1E-15);

			}
		}
		a.delete();
		b.delete();
	}
}

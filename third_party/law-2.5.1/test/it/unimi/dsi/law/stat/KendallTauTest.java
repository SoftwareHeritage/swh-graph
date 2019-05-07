package it.unimi.dsi.law.stat;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

//RELEASE-STATUS: DIST

public class KendallTauTest {
	private final static double[] ordered = { 0.0, 1.0, 2.0, 3.0, 4.0 };
	private final static double[] reverse = { 4.0, 3.0, 2.0, 1.0, 0.0 };
	private final static double[] reverseButOne = { 10.0, 9.0, 7.0, 8.0, 6.0 };
	private final static double[] allOnes = { 1.0, 1.0, 1.0 };
	private final static double[] allZeroes = { 0.0, 0.0, 0.0 };

	/** Computes Kendall's τ by brute-force enumeration of all pairs (<var>i</var>, <var>j</var>), <var>i</var>&lt;<var>j</var>.
	 *
	 * @param v0 the first score vector.
	 * @param v1 the second score vector.
	 * @return Kendall's τ.
	 */
	public static double compute(final double[] v0, final double[] v1) {
		long sum = 0, u = 0, v = 0, tot = (v0.length * (v0.length - 1L)) / 2;
		for(int i = 0; i < v0.length; i++) {
			for(int j = i + 1; j < v0.length; j++) {
				if (v0[i] == v0[j]) u++;
				if (v1[i] == v1[j]) v++;
				if (v0[i] < v0[j] && v1[i] < v1[j] || v0[i] > v0[j] && v1[i] > v1[j]) sum++;
				else if (v0[i] < v0[j] && v1[i] > v1[j] || v0[i] > v0[j] && v1[i] < v1[j]) sum--;
			}
		}
		return Math.min(1, Math.max(-1, sum / (Math.sqrt((double) (tot - u) * (double) (tot - v)))));
	}

	@Test
	public void testComputeOrdered() {
		double expResult = compute(ordered, ordered); // (10.0 - 0.0) / 10.0;
		double result = KendallTau.INSTANCE.compute(ordered, ordered);
		assertEquals(expResult, result, 0.0);
	}

	@Test
	public void testComputeWithReverse() {
		double expResult = compute(ordered, reverse);//(0 - 10.0) / 10.0;
		double result = KendallTau.INSTANCE.compute(ordered, reverse);
		assertEquals(expResult, result, 0.0);
	}

	@Test
	public void testComputeWithReverseButOne() {
		double expResult = compute(ordered, reverseButOne);//(1.0 - 9.0) / 10.0;
		double result = KendallTau.INSTANCE.compute(ordered, reverseButOne);
		assertEquals(expResult, result, 0.0);
	}

	@Test
	public void testRandom() {
		XoRoShiRo128PlusRandom XoRoShiRo128PlusRandom = new XoRoShiRo128PlusRandom(0);
		final double v0[] = new double[1000];
		final double v1[] = new double[1000];
		for(int i = v0.length; i-- != 0;) {
			v0[i] = XoRoShiRo128PlusRandom.nextDouble();
			v1[i] = XoRoShiRo128PlusRandom.nextDouble();
		}
		double expResult = compute(v0, v1);
		double result = KendallTau.INSTANCE.compute(v0, v1);
		assertEquals(expResult, result, 1E-9);
		DoubleArrays.reverse(v0);
		DoubleArrays.reverse(v1);
		assertEquals(expResult, KendallTau.INSTANCE.compute(v0, v1), 1E-9);
	}

	@Test
	public void testRandomWithTies() {
		XoRoShiRo128PlusRandom XoRoShiRo128PlusRandom = new XoRoShiRo128PlusRandom(0);
		final double v0[] = new double[1000];
		final double v1[] = new double[1000];
		for(int i = v0.length; i-- != 0;) {
			v0[i] = XoRoShiRo128PlusRandom.nextInt(10);
			v1[i] = XoRoShiRo128PlusRandom.nextInt(10);
		}
		double expResult = compute(v0, v1);
		double result = KendallTau.INSTANCE.compute(v0, v1);
		assertEquals(expResult, result, 1E-9);
		DoubleArrays.reverse(v0);
		DoubleArrays.reverse(v1);
		assertEquals(expResult, KendallTau.INSTANCE.compute(v0, v1), 1E-9);
	}


	@Test
	public void testAllTies() {
		assertEquals(1.0, KendallTau.INSTANCE.compute(allOnes, allZeroes), 0.0);
	}

	@Test
	public void testOneAllTies() {
		assertTrue(Double.isNaN(KendallTau.INSTANCE.compute(allOnes, new double[] { 0.0, 1.0, 2.0 })));
	}

	@Test
	public void testInputType() throws IOException {
		File a = File.createTempFile(KendallTauTest.class.getSimpleName(), "a");
		a.deleteOnExit();
		File b = File.createTempFile(KendallTauTest.class.getSimpleName(), "b");
		b.deleteOnExit();
		BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeInts(new int[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, KendallTau.INSTANCE.computeInts(a.toString(), b.toString()), 0);
		assertEquals(-1, KendallTau.INSTANCE.computeInts(a.toString(), b.toString(), true), 0);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Integer.class, Integer.MAX_VALUE), 0);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Integer.class, true, Integer.MAX_VALUE), 0);
		BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, KendallTau.INSTANCE.computeInts(a.toString(), b.toString()), 0);
		assertEquals(1, KendallTau.INSTANCE.computeInts(a.toString(), b.toString(), true), 0);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Integer.class, Integer.MAX_VALUE), 0);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Integer.class, true, Integer.MAX_VALUE), 0);

		BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, KendallTau.INSTANCE.computeLongs(a.toString(), b.toString()), 0);
		assertEquals(-1, KendallTau.INSTANCE.computeLongs(a.toString(), b.toString(), true), 0);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Long.class, b.toString(), Long.class, Integer.MAX_VALUE), 0);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Long.class, b.toString(), Long.class, true, Integer.MAX_VALUE), 0);
		BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, KendallTau.INSTANCE.computeLongs(a.toString(), b.toString()), 0);
		assertEquals(1, KendallTau.INSTANCE.computeLongs(a.toString(), b.toString(), true), 0);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Long.class, b.toString(), Long.class, Integer.MAX_VALUE), 0);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Long.class, b.toString(), Long.class, true, Integer.MAX_VALUE), 0);

		BinIO.storeFloats(new float[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeFloats(new float[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, KendallTau.INSTANCE.computeFloats(a.toString(), b.toString()), 0);
		assertEquals(-1, KendallTau.INSTANCE.computeFloats(a.toString(), b.toString(), true), 0);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Float.class, b.toString(), Float.class, Integer.MAX_VALUE), 0);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Float.class, b.toString(), Float.class, true, Integer.MAX_VALUE), 0);
		BinIO.storeFloats(new float[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, KendallTau.INSTANCE.computeFloats(a.toString(), b.toString()), 0);
		assertEquals(1, KendallTau.INSTANCE.computeFloats(a.toString(), b.toString(), true), 0);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Float.class, b.toString(), Float.class, Integer.MAX_VALUE), 0);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Float.class, b.toString(), Float.class, true, Integer.MAX_VALUE), 0);

		BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeDoubles(new double[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, KendallTau.INSTANCE.computeDoubles(a.toString(), b.toString()), 0);
		assertEquals(-1, KendallTau.INSTANCE.computeDoubles(a.toString(), b.toString(), true), 0);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Double.class, b.toString(), Double.class, Integer.MAX_VALUE), 0);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Double.class, b.toString(), Double.class, true, Integer.MAX_VALUE), 0);
		BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, KendallTau.INSTANCE.computeDoubles(a.toString(), b.toString(), true), 0);
		assertEquals(1, KendallTau.INSTANCE.computeDoubles(a.toString(), b.toString()), 0);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Double.class, b.toString(), Double.class, Integer.MAX_VALUE), 0);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Double.class, b.toString(), Double.class, true, Integer.MAX_VALUE), 0);

		BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeDoubles(new double[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Double.class, Integer.MAX_VALUE), 0);
		BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Double.class, Integer.MAX_VALUE), 0);

		BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), Double.class, b.toString(), Long.class, Integer.MAX_VALUE), 0);
		BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), Double.class, b.toString(), Long.class, Integer.MAX_VALUE), 0);

		TextIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, KendallTau.INSTANCE.compute(a.toString(), String.class, b.toString(), Long.class, Integer.MAX_VALUE), 0);
		BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, KendallTau.INSTANCE.compute(a.toString(), String.class, b.toString(), Long.class, Integer.MAX_VALUE), 0);

		a.delete();
		b.delete();
	}
}

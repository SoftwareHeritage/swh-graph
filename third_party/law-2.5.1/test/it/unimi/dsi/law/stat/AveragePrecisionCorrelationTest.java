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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;

//RELEASE-STATUS: DIST

public class AveragePrecisionCorrelationTest {

	private final static double[] ordered = { 0.0, 1.0, 2.0, 3.0, 4.0 };
	private final static double[] reverse = { 4.0, 3.0, 2.0, 1.0, 0.0 };
	private final static double[] reverseButOne = { 10.0, 9.0, 7.0, 8.0, 6.0 };

	public double compute(double[] v0, double[] v1) {
		final int length = v0.length;
		final int[] perm = Util.identity(length);
		DoubleArrays.radixSortIndirect(perm, v1, true);
		IntArrays.reverse(perm);

		double p = 0;
		for(int i = 0; i < length; i++)
			for(int j = i + 1; j < length; j++)
				if (v0[perm[i]] > v0[perm[j]]) p += 1.0 / j;

		p /= length - 1;
		return 2 * p - 1;
	}

	@Test
	public void testComputeOrdered() {
		double expResult = this.compute(ordered, ordered);
		double result = AveragePrecisionCorrelation.INSTANCE.compute(ordered, ordered);
		assertEquals(expResult, result, 0.0);
	}

	@Test
	public void testComputeWithReverse() {
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(ordered, reverse), 1E-14);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(reverse, ordered), 1E-14);
	}

	@Test
	public void testComputeWithReverseButOne() {
		double expResult = this.compute(ordered, reverseButOne);
		double result = AveragePrecisionCorrelation.INSTANCE.compute(ordered, reverseButOne);
		assertEquals(expResult, result, 1E-14);
	}

	@Test
	public void testRandom() {
		final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(1);
		final double[] d = new double[100];
		for(int i = 100; i-- != 0;) d[i] = i;
		final double[] e = d.clone();
		DoubleArrays.shuffle(d,  random);
		DoubleArrays.shuffle(e,  random);

		double expResult = this.compute(d, e);
		double result = AveragePrecisionCorrelation.INSTANCE.compute(d, e);
		assertEquals(expResult, result, 1E-14);
	}

	@Test
	public void test() {
		double p[] = new double[1000];
		for(int i = p.length; i-- != 0;) p[i] = Math.random();
		double q[] = new double[p.length];
		for(int i = q.length; i-- != 0;) q[i] = Math.random();
		double expResult = this.compute(p, q);
		double result = AveragePrecisionCorrelation.INSTANCE.compute(p, q);
		assertEquals(expResult, result, 0.00000001);
	}

	@Test
	public void testInputType() throws IOException {
		File a = File.createTempFile(AveragePrecisionCorrelationTest.class.getSimpleName(), "a");
		a.deleteOnExit();
		File b = File.createTempFile(AveragePrecisionCorrelationTest.class.getSimpleName(), "b");
		b.deleteOnExit();
		BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeInts(new int[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.computeInts(a.toString(), b.toString(), false), 1E-14);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Integer.class, false), 1E-14);
		BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.computeInts(a.toString(), b.toString(), false), 1E-14);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Integer.class, false), 1E-14);

		BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.computeLongs(a.toString(), b.toString(), false), 1E-14);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Long.class, b.toString(), Long.class, false), 1E-14);
		BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.computeLongs(a.toString(), b.toString(), false), 1E-14);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Long.class, b.toString(), Long.class, false), 1E-14);

		BinIO.storeFloats(new float[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeFloats(new float[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.computeFloats(a.toString(), b.toString(), false), 1E-14);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Float.class, b.toString(), Float.class, false), 1E-14);
		BinIO.storeFloats(new float[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.computeFloats(a.toString(), b.toString(), false), 1E-14);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Float.class, b.toString(), Float.class, false), 1E-14);

		BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeDoubles(new double[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.computeDoubles(a.toString(), b.toString(), false), 1E-14);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Double.class, b.toString(), Double.class, false), 1E-14);
		BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.computeDoubles(a.toString(), b.toString(), false), 1E-14);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Double.class, b.toString(), Double.class, false), 1E-14);

		BinIO.storeInts(new int[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeDoubles(new double[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Double.class, false), 1E-14);
		BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Integer.class, b.toString(), Double.class, false), 1E-14);

		BinIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Double.class, b.toString(), Long.class, false), 1E-14);
		BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), Double.class, b.toString(), Long.class, false), 1E-14);

		TextIO.storeDoubles(new double[] { 0, 1, 2, 3, 4 }, a);
		BinIO.storeLongs(new long[] { 4, 3, 2, 1, 0 }, b);
		assertEquals(-1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), String.class, b.toString(), Long.class, false), 1E-14);
		BinIO.storeLongs(new long[] { 0, 1, 2, 3, 4 }, b);
		assertEquals(1, AveragePrecisionCorrelation.INSTANCE.compute(a.toString(), String.class, b.toString(), Long.class, false), 1E-14);

		a.delete();
		b.delete();
	}
}

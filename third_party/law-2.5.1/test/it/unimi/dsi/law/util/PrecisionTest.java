package it.unimi.dsi.law.util;

/*
 * Copyright (C) 2008-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;

import it.unimi.dsi.util.XoRoShiRo128PlusRandom;



//RELEASE-STATUS: DIST

public class PrecisionTest {


	@Test
	public void testValue() {
		assertEquals(Math.PI, Precision.truncate(Math.PI, 10), Math.pow(2, -10));
		assertEquals(Math.PI, Precision.truncate(Math.PI, 13), Math.pow(2, -13));
		assertEquals(Math.PI, Precision.truncate(Math.PI, 17), Math.pow(2, -17));
	}

	@Test
	public void testOnlyFractional() {
		assertEquals(10, Precision.truncate(10, 0), 0);
		assertEquals(10, Precision.truncate(10, 1), 0);
		assertEquals(10, Precision.truncate(10.5, 0), 0);
		assertEquals(10.5, Precision.truncate(10.5, 1), 0);
	}

	@Test
	public void testBin() {
		double[] v = new double[] { 0.5, 1.5 };
		Precision.truncate(v, 2);
		assertEquals(0.5, v[0], Math.pow(2, -2));
		assertEquals(1.5, v[1], Math.pow(2, -2));
		v[0] = 0.25;
		v[1] = 1.25;
		Precision.truncate(v, 1);
		assertEquals(0.25, v[0], Math.pow(2, Integer.MAX_VALUE));
		assertEquals(1.25, v[1], Math.pow(2, Integer.MAX_VALUE));
	}


	private static final BigDecimal TWO = new BigDecimal(2);

	public static double mockTruncate(double value, int numberOfSignificantDigits) {
		final BigDecimal powerOfTwo = numberOfSignificantDigits < 0 ?
				BigDecimal.ONE.divide(TWO.pow(- numberOfSignificantDigits)) : TWO.pow(numberOfSignificantDigits);
		final BigDecimal valueAsBigDecimal = new BigDecimal(value);
		final BigInteger x = valueAsBigDecimal.multiply(powerOfTwo).toBigInteger();
		final BigDecimal result = new BigDecimal(x).divide(powerOfTwo);
		return result.doubleValue();
	}

	@Test
	public void testTruncate() {
		final XoRoShiRo128PlusRandom r = new XoRoShiRo128PlusRandom(0);
		for (int i = 0; i < 1000; i++) {
			final double d = Math.atan(r.nextDouble());
			final int k = r.nextInt(1024);
			assertEquals(mockTruncate(d, k), Precision.truncate(d, k), 0);
		}

		for (int i = 0; i < 1000; i++) {
			final double d = - Math.atan(r.nextDouble());
			final int k = r.nextInt(1024);
			assertEquals(mockTruncate(d, k), Precision.truncate(d, k), 0);
		}
	}

	@Test
	public void testNegative() {
		final XoRoShiRo128PlusRandom r = new XoRoShiRo128PlusRandom(0);
		for (int i = 0; i < 1000; i++) {
			final int d = r.nextInt(10000);
			final int k = r.nextInt(20);
			final double t = Precision.truncate(d, -k);
			assertEquals(Math.floor(t), t, 0);
			assertEquals(d & (-1 << k), (int)t);
		}

		for (int i = 0; i < 1000; i++) {
			final int d = - r.nextInt(10000);
			final int k = r.nextInt(20);
			final double t = Precision.truncate(d, -k);
			assertEquals(Math.floor(t), t, 0);
			assertEquals(mockTruncate(d, -k), t, 0);
		}
	}

	@Test
	public void testUnchanged() {
		final XoRoShiRo128PlusRandom r = new XoRoShiRo128PlusRandom(0);
		for (int i = 0; i < 1000; i++) {
			final double d = 100 * Math.atan(r.nextDouble());
			assertEquals(d, Precision.truncate(d, Integer.MAX_VALUE), 0);
		}
		for (int i = 0; i < 1000; i++) {
			final double d = -100 * Math.atan(r.nextDouble());
			assertEquals(d, Precision.truncate(d, Integer.MAX_VALUE), 0);
		}
	}
}

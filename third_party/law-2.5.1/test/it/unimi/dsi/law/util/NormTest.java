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

import org.junit.Test;



//RELEASE-STATUS: DIST

public class NormTest {

	private final double[] a = {  0.0, -1.0/2.0,  0.0, 1.0/2.0 };
	private final double[] b = { -1.0/3.0,  0.0, -1.0/3.0, 0.0 };

	@Test
	public void testL1ComputeABdouble() {
		double expResult = 1.0 + 2.0 / 3.0;
		double result = Norm.L_1.compute(a, b);
		assertEquals(expResult, result, .0);
	}

	@Test
	public void testL1ComputeAdouble() {
		double expResult = 1.0;
		double result = Norm.L_1.compute(a);
		assertEquals(expResult, result, .0);
	}

	@Test
	public void testL1Normalize() {
		double[] v = { 1, 2, -3, 4.5, -5.5 };
		double expResult = 1.0;
		Norm.L_1.normalize(v, expResult);
		double result = Norm.L_1.compute(v);
		assertEquals(expResult, result, .0);
	}

	@Test
	public void testL2ComputeABdouble() {
		double expResult = Math.sqrt(13.0 / 18.0);
		double result = Norm.L_2.compute(a, b);
		assertEquals(expResult, result, .0);
	}

	@Test
	public void testL2ComputeAdouble() {
		double expResult = Math.sqrt(1.0 / 2.0);
		double result = Norm.L_2.compute(a);
		assertEquals(expResult, result, .0);
	}

	@Test
	public void testL2Normalize() {
		double[] v = { 1, 2, -3, 4.5, -5.5 };
		double expResult = 2.0;
		Norm.L_2.normalize(v, expResult);
		double result = Norm.L_2.compute(v);
		assertEquals(expResult, result, .0);
	}

	@Test
	public void testLIComputeABdouble() {
		double expResult = 1 / 2.;
		double result = Norm.L_INFINITY.compute(a, b);
		assertEquals(expResult, result, .0);
	}

	@Test
	public void testLIComputeAdouble() {
		double expResult = 1 / 2.;
		double result = Norm.L_INFINITY.compute(a);
		assertEquals(expResult, result, .0);
	}

	@Test
	public void testLINormalize() {
		double[] v = { 1, 2, -3, 4.5, -5.5 };
		double expResult = 2.0;
		Norm.L_INFINITY.normalize(v, expResult);
		double result = Norm.L_INFINITY.compute(v);
		assertEquals(expResult, result, .0);
	}
}

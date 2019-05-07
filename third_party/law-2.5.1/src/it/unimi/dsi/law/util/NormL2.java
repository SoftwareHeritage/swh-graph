package it.unimi.dsi.law.util;

/*
 * Copyright (C) 2005-2019 Roberto Posenato and Sebastiano Vigna
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
 */

// RELEASE-STATUS: DIST

/** Static methods that compute &#x2113;<sub>2</sub> norms.
 * @deprecated Use {@link Norm#L_2}. */

@Deprecated
public class NormL2 {

	private NormL2() {}

	public static double compute(final double[] v) {
		double sumOfSquares = 0, c = 0;
		for(int i = v.length ; i-- != 0;) {
			final double y = (v[i] * v[i]) - c;
			final double t = sumOfSquares + y;
			c = (t - sumOfSquares) - y;
			sumOfSquares = t;
		}
		return Math.sqrt(sumOfSquares);
	}


	public static double compute(final double[] v, final double[] w) {
		if (v.length != w.length) throw new IllegalArgumentException("The two vectors have different sizes: " + v.length + " != " + w.length);

		double sumOfSquares = 0, c = 0;
		for(int i = v.length; i-- != 0;) {
			final double y = (v[i] - w[i]) * (v[i] - w[i]) - c;
			final double t = sumOfSquares + y;
			c = (t - sumOfSquares) - y;
			sumOfSquares = t;
		}
		return Math.sqrt(sumOfSquares);
	}

	public static void normalize(final double[] v, final double norm) {
		if (norm < 0) throw new IllegalArgumentException("Negative norm: " + norm);
		double c = norm / Norm.L_2.compute(v);
		for (int i = v.length; i-- != 0;) v[i] *= c;
	}
}

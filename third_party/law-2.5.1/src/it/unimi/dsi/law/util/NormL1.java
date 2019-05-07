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

//RELEASE-STATUS: DIST

/** Static methods that compute &#x2113;<sub>1</sub> norms.
 * @deprecated Use {@link Norm#L_1}.
 */

@Deprecated
public class NormL1 {

	private NormL1() {}

	public static double compute(final double[] v) {
		double normL1 = 0, c = 0;
		for (int i = v.length; i-- != 0;) {
			final double y = Math.abs(v[i]) - c;
			final double t = normL1 + y;
			c = (t - normL1) - y;
			normL1 = t;
		}
		return normL1;
	}

	public static double compute(double[] v, double[] w) {
		if (v.length != w.length) throw new IllegalArgumentException("The two vectors have different sizes: " + v.length + " != " + w.length);

		double normL1 = 0, c = 0;
		for (int i = v.length; i-- != 0;) {
			final double y = Math.abs(v[i] - w[i]) - c;
			final double t = normL1 + y;
			c = (t - normL1) - y;
			normL1 = t;
		}
		return normL1;
	}

	public static void normalize(final double[] v, final double norm) {
		if (norm < 0) throw new IllegalArgumentException("Negative norm: " + norm);
		double c = norm / NormL1.compute(v);
		for (int i = v.length; i-- != 0;) v[i] *= c;
	}
}

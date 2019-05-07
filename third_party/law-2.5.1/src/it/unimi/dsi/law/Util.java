package it.unimi.dsi.law;

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

// RELEASE-STATUS: DIST

/** A static container of utility methods for all LAW software. */

public final class Util {
	/** Computes falling powers.
	 *
	 * @param n the base of the power.
	 * @param k the falling power.
	 * @return <code>n</code>(<code>n</code> &minus; 1)(<code>n</code> &minus; 2)&#x22ef;(<code>n</code> &minus; <code>k</code> + 1).
	 */

	public static double falling(final int n, final int k) {
		if (k > n) return 0;
		if (k == 0) return 1;
		double result = n;
		for(int i = 1; i < k; i++) result *= n - i;
		return result;
	}
}

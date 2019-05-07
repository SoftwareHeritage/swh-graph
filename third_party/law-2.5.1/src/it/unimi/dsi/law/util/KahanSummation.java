package it.unimi.dsi.law.util;

/*
 * Copyright (C) 2011-2019 Sebastiano Vigna
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

/** <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan's
 * summation algorithm</a> encapsulated in an object.  */

public class KahanSummation {
	/** The current value of the sum. */
	private double value;
	/** The current correction. */
	private double c;

	/** Adds a value.
	 * @param v the value to be added to the sum.
	 */
	public void add(final double v) {
		final double y = v - c;
		final double t = value + y;
		c = (t - value) - y;
		value = t;
	}

	/** Returns the sum computed so far.
	 * @return the sum computed so far.
	 */
	public double value() {
		return value;
	}

	/** Resets the current value and correction to zero. */
	public void reset() {
		value = c = 0;
	}
}

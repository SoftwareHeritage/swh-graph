package it.unimi.dsi.law.util;

/*
 * Copyright (C) 2006-2019 Paolo Boldi, Roberto Posenato, Massimo Santini and Sebastiano Vigna
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

/**
 * A set of commodity methods to manipulate precision of doubles.
 *
 */
public class Precision {

	/** Truncates the given double value to the given number of fractional binary digits. This is
	 *  equivalent to multiplying <code>value</code> by 2<sup><code>significantBinaryDigits</code></sup>,
	 *  taking the floor and then multiplying the result by 2<sup><code>-significantBinaryDigits</code></sup>
	 *  (the computation should be performed in arbitrary precision). By choice, this method does not apply any kind of rounding.
	 *
	 *  <p>Note that <code>significantBinaryDigits</code> can be negative: in that case, on positive
	 *  values the method is equivalent to applying the mask <code>-1L << -significantBinaryDigits</code> to the integer part of <code>value</code>.
	 *
	 *  <p>As an example, if you have an estimate <var>v</var> &pm; <var>e</var>, the right value to be passed for <var>v</var> is
	 *  <code>Math.floor(-Math.log(e)/Math.log(2))-1</code>.
	 *
	 * @param value the value to be truncated.
	 * @param significantFractionalBinaryDigits the number of significant fractional binary digits ({@link Integer#MAX_VALUE} causes <code>value</code> to be returned unmodified).
	 * @return the truncated value.
	 */
	public static double truncate(final double value, final int significantFractionalBinaryDigits) {
		final long bits = Double.doubleToLongBits(value);
		// 52 - the exponent.
		final int negExponent = - (int)((bits >> 52) & 0x7FFL) + 1075;
		// The number of digits lost at the end of the significand.
		final long lostDigits = (negExponent - significantFractionalBinaryDigits - (negExponent == 0 ? 1 : 0));
		if (lostDigits <= 0) return value;
		if (lostDigits > 52) return 0;
		return Double.longBitsToDouble(bits & (-1L << lostDigits));
	}

	/** Applies {@link #truncate(double, int)} to the given array.
	 *
	 * <p><strong>Warning</strong>: previous implementations of this method used the special value -1 to indicate
	 * that <code>value</code> was to be left unchanged. The current version uses {@link Integer#MAX_VALUE} to this purpose.
	 *
	 * @param value an array.
	 * @param significantFractionalBinaryDigits the number of significant fractional binary digits ({@link Integer#MAX_VALUE} causes the contents of <code>value</code> to be returned unmodified).
	 * @return <code>value</code>.
	 * @see #truncate(double, int)
	 */
	public static double[] truncate(final double[] value, final int significantFractionalBinaryDigits) {
		if (significantFractionalBinaryDigits != Integer.MAX_VALUE) for (int i = value.length; i-- != 0;) value[i] = truncate(value[i], significantFractionalBinaryDigits);
		return value;
	}
}

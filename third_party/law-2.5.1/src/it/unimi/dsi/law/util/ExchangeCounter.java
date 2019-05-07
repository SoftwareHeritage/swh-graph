package it.unimi.dsi.law.util;

/*
 * Copyright (C) 2004-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.law.stat.KendallTau;

// RELEASE-STATUS: DIST

/** Computes the number of discordances between two score vectors
 * using Knight's O(<var>n</var>&nbsp;log&nbsp;<var>n</var>)
 * MergeSort-based algorithm.
 *
 * <P>The number of <em>discordances</em> between two score vectors is the number
 * of unordered pairs whose mutual relationship is opposite in the two orders (ties in either side are not counted).
 *
 * <P>The computation of the number of discordances is the most onerous step
 * in {@linkplain KendallTau the computation of Kendall's &tau;}
 * It is possible to compute the
 * number of discordances trivially using BubbleSort and counting the exchanges that are
 * necessary to turn from the ranking induced by the first score vector into the ranking induced by the second
 * score vector, but Knight noted that the same can be done
 * in time O(<var>n</var> log <var>n</var>) using a stable sorting algorithm [William R. Knight,
 * &lduo;A Computer Method for Calculating Kendall's Tau with Ungrouped Data&rdquo;, <i>J. Amer. Statist. Assoc.</i>,
 * 61(314):436&minus;439, 1966].
 *
 * <P>This class makes it possible to count the number of exchanges that will change a given ranking, specified by
 * an array of integers, into another order, specified by a score vector.
 * You must {@linkplain #ExchangeCounter(int[], double[]) creates an exchange counter} first,
 * and then invoke {@link #count()}. You are welcome to use a one-liner (e.g., <code>new ExchangeCounter(v, c).count()</code>),
 * so that the large support array allocated by an instance of this class is collected quickly.
 * Optionally, {@linkplain #ExchangeCounter(int[], double[], int[]) you can pass a support array explicitly}.
 *
 * <P>The slightly awkward structure is due to the necessity (in the computation of Kendall's &tau;)
 * of computing the first order externally, and to avoid passing around several additional parameters in recursive
 * calls.
 */
public class ExchangeCounter {
	/** Below this number of elements we use insertion sort. */
	private final static int SMALL = 32;
	/** A support array used by MergeSort. Must be at least as large as {@link #perm}. */
	private final int[] temp;
	/** The score vector used to perform comparisons. */
	private final double[] v;
	/** An array of integers (representing a previously built order). */
	private final int perm[];

	/** Creates a new exchange counter with a provided support array.
	 *
	 * <P>This constructor avoids the need to allocate a support array, in case one is already available.
	 *
	 * @param perm the array to be sorted.
	 * @param v the score vector used to compare the element of {@code perm}.
	 * @param support an array that will be used as temporary storage during the computation (its content will be erased);
	 * must be at least as long as <code>a</code>.
	 */
	public ExchangeCounter(final int perm[], final double[] v, final int[] support) {
		this.perm = perm;
		this.v = v;
		if (support.length < perm.length) throw new IllegalArgumentException("The support array length (" + support.length + ") is smaller than the main array length (" + perm.length + ")");
		this.temp = support;
	}

	/** Creates a new exchange counter.
	 *
	 * @param perm the array to be sorted.
	 * @param v the score vector used to compare the element of {@code perm}.
	 */
	public ExchangeCounter(final int perm[], final double[] v) {
		this(perm, v, new int[perm.length]);
	}

	/** Computes the number of exchanges.
	 *
	 * <P>Note that a call to this method will actually sort the permutation at creation time.
	 *
	 * @return the number of exchanges that will order the permutation provided at creation time using the score vector
	 * provided at creation time.
	 */
	public long count() {
		return count(0, perm.length);
	}

	/** Orders a subarray of {@link #perm} and returns the number of exchanges.
	 *
	 * @param offset the starting element to order.
	 * @param length the number of elements to order.
	 * @return the number of exchanges in the subarray.
	 */


	private long count(final int offset, final int length) {
		long exchanges = 0;
		final int[] perm = this.perm;

		if (length < SMALL) {
			final int end = offset + length;
			for (int i = offset; ++i < end;) {
				int t = perm[i];
				int j = i;
				for (int u = perm[j - 1]; v[t] < v[u]; u = perm[j - 1]) {
					exchanges++;
					perm[j--] = u;
					if (offset == j) break;
				}
				perm[j] = t;
			}

			return exchanges;
		}

		final int length0 = length / 2, length1 = length - length / 2, middle = offset + length0;
		exchanges += count(offset, length0);
		exchanges += count(middle, length1);

		/* If the last element of the first subarray is smaller than the first element of
		 * the second subarray, there is nothing to do.  */
		if (v[perm[middle - 1]] >= v[perm[middle]]) {
			// We merge the lists into temp, adding the number of forward moves to exchanges.
			int i = 0, j = 0, k = 0;
			while(j < length0 && k < length1) {
				if (v[perm[offset + j]] <= v[perm[middle + k]]) {
					temp[i] = perm[offset + j++];
				}
				else {
					temp[i] = perm[middle + k++];
					exchanges += length0 - j;
				}
				i++;
			}

			System.arraycopy(perm, offset + j, perm, offset + i, length0 - j);
			System.arraycopy(temp, 0, perm, offset, i);
		}
		return exchanges;
	}
}

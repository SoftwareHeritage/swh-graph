package it.unimi.dsi.law.util;

/*
 * Copyright (C) 2013-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.ints.Int2DoubleFunction;

// RELEASE-STATUS: DIST

/** Computes the weight of discordances using a generalisation of Knight's algorithm. */

public class ExchangeWeigher {
	/** The function used to weigh elements. */
	private final Int2DoubleFunction weigher;
	/** A rank on the elements of {@link #perm}. The weight is computed by applying the {@link #weigher} on the rank. */
	private final int[] rank;
	/** The score vector used to perform comparisons. */
	private final double[] v;
	/** A support array used by MergeSort. Must be at least as large as {@link #perm}. */
	private final int[] temp;
	/** An array of integers representing a previously built order. */
	private final int perm[];
	/** Whether combining weights multiplicatively, rather than additively, which is the default. */
	private final boolean multiplicative;
	/** The currently accumulated exchange weight. */
	private double exchangeWeight;

	/** Creates a new exchange weigher.
	 *
	 * @param weigher the function used to weight indices.
	 * @param perm the array to be sorted.
	 * @param v the score vector used to compare the element of {@code perm}.
	 * @param rank a rank on the indices.
	 * @param multiplicative whether to combine weights multiplicatively, rather than additively, which is the default.
	 * @param support an array that will be used as temporary storage during the computation; its content will be erased, and it
	 * must be at least as long as {@code perm}. If {@code null}, it will be allocated.
	 */
	public ExchangeWeigher(final Int2DoubleFunction weigher, final int[] perm, final double[] v, final int[] rank, final boolean multiplicative, final int[] support) {
		this.weigher = weigher;
		this.perm = perm;
		this.v = v;
		this.rank = rank;
		this.multiplicative = multiplicative;
		if (rank.length != perm.length) throw new IllegalArgumentException("The permutation array length (" + perm.length + ") and the rank array length (" + rank.length + ") do not match");
		if (support != null && support.length < perm.length) throw new IllegalArgumentException("The support array length (" + support.length + ") is smaller than the main array length (" + perm.length + ")");
		this.temp = support == null ? new int[perm.length] : support;
	}

	/** Computes the weight of exchanges for the current data.
	 *
	 * <P>Note that a call to this method will actually order the array
	 * provided at creation time using the comparator provided
	 * at creation time; thus, subsequent calls will always return 1.
	 *
	 * @return the weight of exchanges that will order the vector provided at creation time using the comparator provided
	 * at creation time.
	 */
	public double weigh() {
		exchangeWeight = 0;
		weigh(0, perm.length);
		return exchangeWeight;
	}

	/** Orders a subarray of {@link #perm} and returns the sum of the weight of its elements.
	 *
	 * @param offset the starting element to order.
	 * @param length the number of elements to order.
	 * @return the weight of the elements in the subarray.
	 */

	private double weigh(final int offset, final int length) {
		/* Using a non-recursive sort for small subarrays gives no noticeable
		 * improvement, as most of the cost is given by floating-point computations. */
		if (length == 1) return weigher.get(rank[perm[offset]]);

		final int length0 = length / 2, length1 = length - length / 2, middle = offset + length0;
		double residual = weigh(offset, length0);
		final double weight = weigh(middle, length1) + residual;

		/* If the last element of the first subarray is larger than or equal to the first element of
		 * the second subarray, then there is nothing to do. */
		if (v[perm[middle - 1]] >= v[perm[middle]]) {
			// We merge the lists into temp, adding the weights of the resulting exchanges.
			int i = 0, j = 0, k = 0;
			while(j < length0 && k < length1) {
				if (v[perm[offset + j]] <= v[perm[middle + k]]) {
					temp[i] = perm[offset + j++];
					residual -= weigher.get(rank[temp[i]]);
				}
				else {
					temp[i] = perm[middle + k++];
					exchangeWeight += multiplicative
							? weigher.get(rank[temp[i]]) * residual
							: weigher.get(rank[temp[i]]) * (length0 - j) + residual;
				}
				i++;
			}

			System.arraycopy(perm, offset + j, perm, offset + i, length0 - j);
			System.arraycopy(temp, 0, perm, offset, i);
		}
		return weight;
	}
}

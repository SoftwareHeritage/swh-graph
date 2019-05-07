package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2003-2017 Paolo Boldi and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.ints.IntIterator;

/** An iterator returning the union of the integers returned by two {@link IntIterator}s.
 *  The two iterators must return integers in an increasing fashion; the resulting
 *  {@link MergedLongIterator} will do the same. Duplicates will be eliminated.
 */

public class MergedLongIterator implements LazyLongIterator {
	/** The first component iterator. */
	private final LazyLongIterator it0;
	/** The second component iterator. */
	private final LazyLongIterator it1;
	/** The maximum number of integers to be still returned. */
	private long n;
	/** The last integer returned by {@link #it0}. */
	private long curr0;
	/** The last integer returned by {@link #it1}. */
	private long curr1;

	/** Creates a new merged iterator by merging two given iterators.
	 *
	 * @param it0 the first (monotonically nondecreasing) component iterator.
	 * @param it1 the second (monotonically nondecreasing) component iterator.
	 */
	public MergedLongIterator(final LazyLongIterator it0, final LazyLongIterator it1) {
		this (it0, it1, Integer.MAX_VALUE);
	}

	/** Creates a new merged iterator by merging two given iterators; the resulting iterator will not emit more than <code>n</code> integers.
	 *
	 * @param it0 the first (monotonically nondecreasing) component iterator.
	 * @param it1 the second (monotonically nondecreasing) component iterator.
	 * @param n the maximum number of integers this merged iterator will return.
	 */
	public MergedLongIterator(final LazyLongIterator it0, final LazyLongIterator it1, final long n) {
		this.it0 = it0;
		this.it1 = it1;
		this.n = n;
		curr0 = it0.nextLong();
		curr1 = it1.nextLong();
	}

	@Override
	public long nextLong() {
		if (n == 0 || curr0 == -1 && curr1 == -1) return -1;
		n--;

		final long result;

		if (curr0 == -1) {
			result = curr1;
			curr1 = it1.nextLong();
		}
		else if (curr1 == -1) {
			result = curr0;
			curr0 = it0.nextLong();
		}
		else if (curr0 < curr1) {
			result = curr0;
			curr0 = it0.nextLong();
		}
		else if (curr0 > curr1) {
			result = curr1;
			curr1 = it1.nextLong();
		}
		else {
			result = curr0;
			curr0 = it0.nextLong();
			curr1 = it1.nextLong();
		}

		return result;
	}

	@Override
	public long skip(final long s) {
		long i;
		for(i = 0; i < s; i++) {
			if (n == 0 || curr0 == -1 && curr1 == -1) break;
			n--;

			if (curr0 == -1) curr1 = it1.nextLong();
			else if (curr1 == -1) curr0 = it0.nextLong();
			else if (curr0 < curr1) curr0 = it0.nextLong();
			else if (curr0 > curr1) curr1 = it1.nextLong();
			else {
				curr0 = it0.nextLong();
				curr1 = it1.nextLong();
			}
		}
		return i;
	}
}

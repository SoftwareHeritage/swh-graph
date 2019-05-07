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

import it.unimi.dsi.fastutil.longs.LongBigArrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** An immutable graph representing the union of two given graphs. Here by &ldquo;union&rdquo;
 *  we mean that an arc will belong to the union iff it belongs to at least one of the two graphs (the number of
 *  nodes of the union is taken to be the maximum among the number of nodes of each graph).
 */
public class UnionImmutableGraph extends ImmutableGraph {
	@SuppressWarnings("unused")
	private static final Logger LOGGER = LoggerFactory.getLogger(Transform.class);
	@SuppressWarnings("unused")
	private static final boolean DEBUG = false;
	@SuppressWarnings("unused")
	private static final boolean ASSERTS = false;

	private final ImmutableGraph g0, g1;
	private final long n0, n1, numNodes;

	/** The node whose successors are cached, or -1 if no successors are currently cached. */
	private final long cachedNode = -1;

	/** The outdegree of the cached node, if any. */
	private long outdegree ;

	/** The successors of the cached node, if any; note that the array might be larger. */
	private long[][] cache = LongBigArrays.EMPTY_BIG_ARRAY;

	/** Creates the union of two given graphs.
	 *
	 * @param g0 the first graph.
	 * @param g1 the second graph.
	 */
	public UnionImmutableGraph(ImmutableGraph g0, ImmutableGraph g1) {
		this.g0 = g0;
		this.g1 = g1;
		n0 = g0.numNodes();
		n1 = g1.numNodes();
		numNodes = Math.max(n0, n1);
	}

	@Override
	public UnionImmutableGraph copy() {
		return new UnionImmutableGraph(g0.copy(), g1.copy());
	}

	@Override
	public NodeIterator nodeIterator(final long from) {

		return new NodeIterator() {
			/** If outdegree is nonnegative, the successors of the current node (this array may be, however, larger). */
			@SuppressWarnings("hiding")
			private long[][] cache = LongBigArrays.EMPTY_BIG_ARRAY;
			/** The outdegree of the current node, or -1 if the successor array for the current node has not been computed yet. */
			@SuppressWarnings("hiding")
			private long outdegree = -1;
			private NodeIterator i0 = from < n0? g0.nodeIterator(from) : null;
			private NodeIterator i1 = from < n1? g1.nodeIterator(from) : null;

			@Override
			public boolean hasNext() {
				return i0 != null && i0.hasNext() || i1 != null && i1.hasNext();
			}

			@Override
			public long nextLong() {
				if (! hasNext()) throw new java.util.NoSuchElementException();
				outdegree = -1;
				long result = -1;
				if (i0 != null) {
					if (i0.hasNext()) result = i0.nextLong();
					else i0 = null;
				}
				if (i1 != null) {
					if (i1.hasNext()) result = i1.nextLong();
					else i1 = null;
				}
				return result;
			}

			@Override
			public long[][] successorBigArray() {
				if (outdegree != -1) return cache;
				if (i0 == null) {
					outdegree = i1.outdegree();
					return cache = i1.successorBigArray();
				}
				if (i1 == null) {
					outdegree = i0.outdegree();
					return cache = i0.successorBigArray();
				}

				MergedLongIterator merge = new MergedLongIterator(i0.successors(), i1.successors());
				outdegree = LazyLongIterators.unwrap(merge, cache);
				long upto, t;
				while ((t = merge.nextLong()) != -1) {
					upto = LongBigArrays.length(cache);
					cache = LongBigArrays.grow(cache, upto + 1);
					LongBigArrays.set(cache, upto++, t);
					outdegree++;
					outdegree += LazyLongIterators.unwrap(merge, cache, upto, LongBigArrays.length(cache) - upto);
				}
				return cache;
			}

			@Override
			public long outdegree() {
				successorBigArray(); // So that the cache is filled up
				return outdegree;
			}

		};

	}

	@Override
	public long numNodes() {
		return numNodes;
	}

	@Override
	public boolean randomAccess() {
		return g0.randomAccess() && g1.randomAccess();
	}

	@Override
	public long[][] successorBigArray(final long x) {
		if (x == cachedNode) return cache;
		final MergedLongIterator merge = new MergedLongIterator(x < n0? g0.successors(x) : LazyLongIterators.EMPTY_ITERATOR, x < n1? g1.successors(x) : LazyLongIterators.EMPTY_ITERATOR);
		outdegree = LazyLongIterators.unwrap(merge, cache);
		long upto, t;
		while ((t = merge.nextLong()) != -1) {
			upto = LongBigArrays.length(cache);
			cache = LongBigArrays.grow(cache, upto + 1);
			LongBigArrays.set(cache, upto++, t);
			outdegree++;
			outdegree += LazyLongIterators.unwrap(merge, cache, upto, LongBigArrays.length(cache) - upto);
		}
		return cache;
	}

	@Override
	public long outdegree(final long x) {
		successorBigArray(x); // So the cache gets filled
		return outdegree;
	}
}

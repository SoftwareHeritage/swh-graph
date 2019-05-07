package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2011-2017 Sebastiano Vigna
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

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.junit.Test;

public class BVGraphSlowTest extends WebGraphTestCase {

	protected final static class BigGraph extends ImmutableSequentialGraph {
		private final long numNodes;
		private final long outdegree;
		private final int step;

		public BigGraph(long numNodes, long outdegree, int step) {
			if (outdegree * step > numNodes) throw new IllegalArgumentException();
			this.numNodes = numNodes;
			this.outdegree = outdegree;
			this.step = step;
		}

		public BigGraph(long outdegree, int step) {
			this(outdegree * step, outdegree, step);
		}

		@Override
		public long numNodes() {
			return numNodes;
		}

		@Override
		public NodeIterator nodeIterator(long from) {
			return new NodeIterator() {
				long next = 0;
				@Override
				public boolean hasNext() {
					return next < numNodes();
				}

				@Override
				public long nextLong() {
					if (! hasNext()) throw new NoSuchElementException();
					return next++;
				}

				@Override
				public long outdegree() {
					return next < 2 ? outdegree : 2;
				}

				@Override
				public LazyLongIterator successors() {
					if (next >= 2) return LazyLongIterators.wrap(new long[] { next - 2, next - 1 });
					else return new AbstractLazyLongIterator() {
						public long i = 0;
						@Override
						public long nextLong() {
							if (i == outdegree) return -1;
							else return i++ * step;
						}
					};
				}
			};
		}
	}

	@Test
	public void testStore() throws IOException {
		final ImmutableGraph graph = new BigGraph(3L << 31, 1L << 30, 4);
		File basename = File.createTempFile(BVGraphTest.class.getSimpleName(), "test");
		BVGraph.store(graph, basename.toString());
		assertEquals(graph, BVGraph.load(basename.toString()));
		assertEquals(graph, BVGraph.loadMapped(basename.toString()));
		assertEquals(graph, BVGraph.loadOffline(basename.toString()));
		deleteGraph(basename);
	}
}

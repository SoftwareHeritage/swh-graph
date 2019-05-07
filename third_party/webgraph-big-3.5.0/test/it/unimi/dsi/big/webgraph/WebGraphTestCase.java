package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2003-2017 Sebastiano Vigna
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
import static org.junit.Assert.assertFalse;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import it.unimi.dsi.big.webgraph.labelling.Label;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.webgraph.BVGraph;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** A JUnit test case providing additional assertions
 * for {@linkplain it.unimi.dsi.big.webgraph.ImmutableGraph immutable graphs}.
 */

public abstract class WebGraphTestCase {

	private static void copy(InputStream in, OutputStream out) throws IOException {
		int c;
		while((c = in.read()) != -1) out.write(c);
		out.close();
	}

	/** Returns a path to a temporary graph that copies a resource graph with given basename.
	 *
	 * @param basename the basename.
	 * @return the graph.
	 * @throws IOException
	 */
	public String getGraphPath(final String basename) throws IOException {
		File file = File.createTempFile(getClass().getSimpleName(), "graph");
		file.delete();

		copy(getClass().getResourceAsStream(basename + BVGraph.GRAPH_EXTENSION), new FileOutputStream(file.getCanonicalPath() + BVGraph.GRAPH_EXTENSION));
		copy(getClass().getResourceAsStream(basename + BVGraph.OFFSETS_EXTENSION), new FileOutputStream(file.getCanonicalPath() + BVGraph.OFFSETS_EXTENSION));
		copy(getClass().getResourceAsStream(basename + BVGraph.PROPERTIES_EXTENSION), new FileOutputStream(file.getCanonicalPath() + BVGraph.PROPERTIES_EXTENSION));

		return file.getCanonicalPath();
	}

	/** Cleans up a temporary graph.
	 *
	 * @param basename the basename.
	 */

	public static void deleteGraph(final String basename) {
		deleteGraph(new File(basename));
	}


	/** Cleans up a temporary graph.
	 *
	 * @param basename the basename.
	 */
	public static void deleteGraph(final File basename) {
		new File(basename + BVGraph.GRAPH_EXTENSION).delete();
		new File(basename + BVGraph.OFFSETS_EXTENSION).delete();
		new File(basename + BVGraph.OFFSETS_BIG_LIST_EXTENSION).delete();
		new File(basename + ImmutableGraph.PROPERTIES_EXTENSION).delete();
	}

	/** Performs a stress-test of an immutable graph. All available methods
	 * for accessing outdegrees and successors are cross-checked.
	 *
	 * @param g the immutable graph to be tested.
	 */

	public static void assertGraph(ImmutableGraph g) {
		NodeIterator nodeIterator0 = g.nodeIterator(), nodeIterator1 = g.nodeIterator();
		long d;
		long[][] s0;
		Label[][] l0;
		LazyLongIterator s1;
		int m = 0;
		long curr;
		// Check that iterator and array methods return the same values in sequential scans.
		for(long i = g.numNodes(); i-- != 0;) {
			curr = nodeIterator0.nextLong();
			assertEquals(curr, nodeIterator1.nextLong());
			d = nodeIterator0.outdegree();
			m += d;
			assertEquals(d, nodeIterator1.outdegree());

			s0 = nodeIterator0.successorBigArray();
			s1 = nodeIterator1.successors();
			for(long k = 0; k < d; k++) assertEquals(LongBigArrays.get(s0, k), s1.nextLong());
			assertEquals(-1, s1.nextLong());

			if (g instanceof ArcLabelledImmutableGraph) {
				l0 = ((ArcLabelledNodeIterator)nodeIterator0).labelBigArray();
				s1 = ((ArcLabelledNodeIterator)nodeIterator1).successors();
				for(long k = 0; k < d; k++) {
					s1.nextLong();
					assertEquals(ObjectBigArrays.get(l0, k), ((LabelledArcIterator)s1).label());
				}
			}

			assertEquals(-1, s1.nextLong());
		}

		try {
			assertEquals(m, g.numArcs());
		}
		catch(UnsupportedOperationException ignore) {} // A graph might not support numArcs().
		assertFalse(nodeIterator0.hasNext());
		assertFalse(nodeIterator1.hasNext());

		if (! g.randomAccess()) return;

		// Check that sequential iterator methods and random methods do coincide.
		String msg;

		for(long s = 0; s < g.numNodes() - 1; s++) {
			nodeIterator1 = g.nodeIterator(s);
			for(long i = g.numNodes() - s; i-- != 0;) {
				curr = nodeIterator1.nextLong();
				msg = "Node " + curr + ", starting from " + s + ":";
				d = g.outdegree(curr);
				assertEquals(msg, d, nodeIterator1.outdegree());
				s0 = g.successorBigArray(curr);
				s1 = nodeIterator1.successors();
				for(long k = 0; k < d; k++) assertEquals(msg, LongBigArrays.get(s0, k), s1.nextLong());
				s1 = g.successors(curr);
				for(long k = 0; k < d; k++) assertEquals(msg, LongBigArrays.get(s0, k), s1.nextLong());
				assertEquals(msg, -1, s1.nextLong());

				if (g instanceof ArcLabelledImmutableGraph) {
					l0 = ((ArcLabelledImmutableGraph)g).labelBigArray(curr);
					s1 = ((ArcLabelledNodeIterator)nodeIterator1).successors();
					for(long k = 0; k < d; k++) {
						s1.nextLong();
						assertEquals(msg, ObjectBigArrays.get(l0, k), ((LabelledArcIterator)s1).label());
					}
					s1 = g.successors(curr);
					for(long k = 0; k < d; k++) {
						s1.nextLong();
						assertEquals(msg, ObjectBigArrays.get(l0, k), ((LabelledArcIterator)s1).label());
					}
					assertEquals(msg, -1, s1.nextLong());
				}
			}
		}

		// Check that cross-access works.

		nodeIterator0 = g.nodeIterator();
		for(long s = 0; s < g.numNodes(); s++) {
			d = g.outdegree(s);
			nodeIterator0.nextLong();
			LazyLongIterator successors = g.successors(s);
			long[][] succ = nodeIterator0.successorBigArray();
			for(long i = 0; i < d; i++) {
				final long t = successors.nextLong();
				assertEquals(LongBigArrays.get(succ, i), t);
				g.outdegree(t);
			}

		}
		// Check copies
		assertEquals(g, g.copy());
	}
}

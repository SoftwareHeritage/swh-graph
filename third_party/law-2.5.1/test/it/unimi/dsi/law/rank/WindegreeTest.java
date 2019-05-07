package it.unimi.dsi.law.rank;

/*
 *  Copyright (C) 2011-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;

//RELEASE-STATUS: DIST

public class WindegreeTest {

	@Test
	public void test() {
		final ImmutableGraph graph = new ArrayListMutableGraph(8, new int[][] { { 0, 1 }, { 2, 1 }, { 2, 3 }, { 4, 5 }, { 5, 6 } }).immutableView();
		float[] coreachable = { 0, 2, 0, 1, 0, 1, 2, 0 };
		double[] expected = { 0, 2 * 2, 0, 1 * 1, 0, 1 * 1, 1 * 2, 0 };
		double[] rank = Windegree.rank(graph, coreachable, false, null);
		System.err.println(Arrays.toString(rank));
		assertArrayEquals(expected, rank, 1E-50);

		// Markovian
		expected = new double[] { 0, (1 + 1./2) * 2, 0, (1./2) * 1, 0, 1 * 1, 1 * 2, 0 };
		rank = Windegree.rank(graph, coreachable, true, null);
		System.err.println(Arrays.toString(rank));
		assertArrayEquals(expected, rank, 1E-50);
}

}

package it.unimi.dsi.law.graph;

/*
 * Copyright (C) 2010-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Random;

import org.junit.Test;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;



//RELEASE-STATUS: DIST

public class DFSTest {
	@Test
	public void testStartPerm() {
		for (int i = 100; i <= 1000; i += 100) {
			ImmutableGraph g = new ArrayListMutableGraph(Transform.symmetrize(new ErdosRenyiGraph(i, .02, 0, false))).immutableView();
			final int[] startPerm = Util.identity(new int[g.numNodes()]);
			Collections.shuffle(IntArrayList.wrap(startPerm), new Random(0));
			ImmutableGraph mg = Transform.map(g, startPerm);


			int[] perm0 = Util.invertPermutationInPlace(DFS.dfsperm(g, startPerm));
			int[] perm1 = Util.invertPermutationInPlace(DFS.dfsperm(mg, Util.identity(i)));

			assertTrue(Transform.map(g, perm0).equals(Transform.map(mg, perm1)));
		}
	}
}

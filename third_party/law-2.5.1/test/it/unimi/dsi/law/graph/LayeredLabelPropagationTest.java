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

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;



//RELEASE-STATUS: DIST

public class LayeredLabelPropagationTest {

	@Test
	public void testStartPerm() throws IOException {

		for(int i = 100; i <= 1000; i += 100) {
			final XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(0);
			final ImmutableGraph g = new ArrayListMutableGraph(Transform.symmetrize(new ErdosRenyiGraph(i, .02, 0,false))).immutableView();
			final int[] startPerm = IntArrays.shuffle(Util.identity(g.numNodes()), random);
			final ImmutableGraph mg = new ArrayListMutableGraph(Transform.map(g, startPerm)).immutableView();

			final LayeredLabelPropagation clustering0 = new LayeredLabelPropagation(g, startPerm,  1, 0, true);
			final LayeredLabelPropagation clustering1 = new LayeredLabelPropagation(mg, Util.identity(g.numNodes()), 1, 0, true);
			final LayeredLabelPropagation clustering2 = new LayeredLabelPropagation(mg, null, 1, 0, true);

			final double[] gammas = { 1/16., 1/32., 1/64., 1/128., 1/256. };
			final int[] perm0 = clustering0.computePermutation(gammas, null, Integer.MAX_VALUE);
			final int[] perm1 = clustering1.computePermutation(gammas, null, Integer.MAX_VALUE);
			final int[] perm2 = clustering2.computePermutation(gammas, null, Integer.MAX_VALUE);

			assertTrue(Arrays.equals(perm1, perm2));

			assertTrue(	Transform.map(g, perm0).equals(Transform.map(mg, perm1)));
		}
	}
}

package it.unimi.dsi.webgraph.examples;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.WebGraphTestCase;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;

/*
 * Copyright (C) 2010-2017 Paolo Boldi and Sebastiano Vigna
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



public class IntegerListImmutableGraphTest extends WebGraphTestCase {

	@Test
	public void test() throws IOException {
		for (int size: new int[] { 5, 10, 100 })
			for (double p: new double[] { .1, .3, .5, .9 }) {

				ErdosRenyiGraph eg = new ErdosRenyiGraph(size, p, true);
				String filename = File.createTempFile(IntegerListImmutableGraphTest.class.getSimpleName(), "test").getAbsolutePath();
				DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename));
				dos.writeInt(eg.numNodes());
				NodeIterator nodeIterator = eg.nodeIterator();
				while (nodeIterator.hasNext()) {
					nodeIterator.nextInt();
					dos.writeInt(nodeIterator.outdegree());
					LazyIntIterator successors = nodeIterator.successors();
					for (;;) {
						int succ = successors.nextInt();
						if (succ < 0) break;
						dos.writeInt(succ);
					}
				}
				dos.close();

				ImmutableGraph graph = IntegerListImmutableGraph.loadOffline(filename);
				assertGraph(graph);
			}
	}

}

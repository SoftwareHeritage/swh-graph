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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.Transform;


//RELEASE-STATUS: DIST

public class RemoveHubsTest {

	@Test
	public void testTwoStar() throws IOException {
		final int n = 100;
		ArrayListMutableGraph g = new ArrayListMutableGraph();
		g.addNodes(n);
		for(int i = 0; i < n-2; i++) {
			g.addArc(n-2, i);
			g.addArc(i, n-1);
		}
		int[] perm = RemoveHubs.largestOutdegree(g.immutableView());
		assertEquals(perm[0], n-1);
		assertEquals(perm[n-1], n-2);

		perm = RemoveHubs.largestIndegree(Transform.transpose(g.immutableView()));
		assertEquals(perm[n - 1], n - 1);

		perm = RemoveHubs.pageRank(Transform.transpose(g.immutableView()));

		assertEquals(perm[0], n-2);
		assertEquals(perm[n-1], n-1);
	}

	@Test
	public void testStar() throws IOException {
		final int n = 100;
		ArrayListMutableGraph g = new ArrayListMutableGraph();
		g.addNodes(n);
		for(int i = 0; i < n-1; i++) {
			g.addArc(n-1, i);
			g.addArc(i, n-1);
		}

		int[] perm = RemoveHubs.symPageRank(g.immutableView(), g.immutableView());
		assertEquals(perm[n-1], n-1);

		char[] slash = new char[(n * (n+1)) / 2 + n];
		int pos = 0;
		for(int i = 0; i < n; i++) {
			for(int j = 0; j < i+1; j++)
				slash[pos++] = '/';
			slash[pos++] = '\n';
		}

		perm = RemoveHubs.url(g.immutableView(), new FastBufferedReader(slash));
		for(int i = 0; i < n; i++)
			assertEquals(perm[i], n-1-i);

		double[] fraction = new double[1];
		fraction[0] = 0.5;
		int[] cut = RemoveHubs.store(g.immutableView(), g.immutableView(), fraction, perm, null, null);
		assertEquals(cut[0], n/2-1);
	}

	@Test
	public void testUrl() throws IOException {
		ArrayListMutableGraph g = new ArrayListMutableGraph();
		g.addNodes(3);
		StringBuffer bf = new StringBuffer();
		bf.append("http://www.skwigly.co.uk/banner\n");
		bf.append("http://www.skwigly.co.uk/\n");
		bf.append("http://www.skwigly.co.uk/dir\n");
		int[] perm = RemoveHubs.url(g.immutableView(), new FastBufferedReader(bf.toString().toCharArray()));
		assertEquals(perm[2], 1);
	}

	@Test
	public void testTwoClique() throws IOException {
		final int n = 100;
		ArrayListMutableGraph g = new ArrayListMutableGraph();
		g.addNodes(2*n);
		for(int i = 0; i < n; i++)
			for(int j = i+1; j < n; j++) {
					g.addArc(i, j);
					g.addArc(j, i);
					g.addArc(i+n, j+n);
					g.addArc(j+n, i+n);
			}

		g.addArc(n-1, n);
		g.addArc(n, n-1);

		int[] perm = RemoveHubs.labelPropagation(Transform.symmetrize(g.immutableView()));

		assertTrue(perm[2*n-1] == n || perm[2*n - 1] == n-1);
		assertTrue(perm[2*n-2] == n || perm[2*n - 2] == n-1);
	}

	@Test
	public void testRandom() {
		final int n = 100000;
		ArrayListMutableGraph g = new ArrayListMutableGraph();
		g.addNodes(n);
		int[] perm = RemoveHubs.random(g.immutableView());
		double sum = 0;
		for(int i = 0; i < n-1; i++)
			if(perm[i] > perm[i + 1])
				sum++;
		sum /= n-1;
		assertTrue(sum < 0.6 && sum > 0.4);
	}

}

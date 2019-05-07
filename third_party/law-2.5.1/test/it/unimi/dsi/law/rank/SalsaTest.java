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

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.slf4j.helpers.NOPLogger;

import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.examples.ErdosRenyiGraph;

//RELEASE-STATUS: DIST

public class SalsaTest {

	@Test
	public void testSalsaConnectedM() {
		final ImmutableGraph graph = new ArrayListMutableGraph(6, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 4 }, { 3, 1 }, { 3, 5 }, { 4, 0 }, { 4, 3 }, { 5, 3 }, { 5, 4 } }).immutableView();
		double[] expected = { 1, 2, 1, 2, 2, 1 };
		Norm.L_1.normalize(expected, 1);
		assertArrayEquals(expected, Salsa.rank(graph, null), 1E-50);
	}

	@Test
	public void testSalsaWithIsolated() {
		final ImmutableGraph graph = new ArrayListMutableGraph(8, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 4 }, { 3, 1 }, { 3, 5 }, { 4, 0 }, { 4, 3 }, { 5, 3 }, { 5, 4 } }).immutableView();
		double[] expected = { 1 * (6./8) / 9, 2 * (6./8) / 9, 1 * (6./8) / 9, 2 * (6./8) / 9, 2 * (6./8) / 9, 1 * (6./8) / 9, 0, 0 };
		assertArrayEquals(expected, Salsa.rank(graph, null), 1E-50);
	}

	@Test
	public void testSalsaNonconnectedM() {
		final ImmutableGraph graph = new ArrayListMutableGraph(6, new int[][] { { 0, 1 }, { 0, 2 }, { 0, 4 }, { 3, 1 }, { 3, 5 }, { 4, 0 }, { 4, 3 }, { 5, 3 } }).immutableView();

		double[] indegree = { 1. / 3, 2. / 5, 1. / 5, 2. / 3, 1. / 5, 1. / 5 };
		double[] ccSize = { 2, 4, 4, 2, 4, 4 };
		double[] expected = new double[indegree.length];
		for (int i = 0; i < indegree.length; i++) expected[i] = indegree[i] * ccSize[i] / 6.0;
		assertArrayEquals(expected, Salsa.rank(graph, null), 1E-50);
	}

	@Test
	public void testCycle() {
		for(int size: new int[] { 100, 1000, 10000 }) {
			final ImmutableGraph bidirectionalCycle = ArrayListMutableGraph.newBidirectionalCycle(size).immutableView();
			final double[] expected = new double[size];
			Arrays.fill(expected, 1);
			Norm.L_1.normalize(expected, 1);
			assertArrayEquals(expected, Salsa.rank(bidirectionalCycle, null), 1E-50);
		}
	}

	@Test
	public void testClique() {
		for(int size: new int[] { 10, 100, 1000 }) {
			final ImmutableGraph clique = ArrayListMutableGraph.newCompleteGraph(size, false).immutableView();
			final double[] expected = new double[size];
			Arrays.fill(expected, 1);
			Norm.L_1.normalize(expected, 1);
			assertArrayEquals(expected, Salsa.rank(clique, null), 1E-50);
		}
	}

	@Test
	public void testRandomSymmetric() throws IOException {
		for(int size: new int[] { 10, 100 }) {
			// TODO refactor when symmetrize will return a copyable graph
			final ImmutableGraph graph = new ArrayListMutableGraph(Transform.symmetrize(new ErdosRenyiGraph(size, .3, 0, false))).immutableView();
			final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(graph, graph, NOPLogger.NOP_LOGGER);
			lsv.norm = Norm.L_1;
			lsv.salsa = true;
			lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(1E-15));

			assertArrayEquals(lsv.rank, Salsa.rank(graph, null), 1E-2);
		}
	}

	@Test
	public void testRandom() throws IOException {
		for(double p: new double[] { 0.1, 0.3, 0.9 }) {
			for(int size: new int[] { 10, 100 }) {
				// TODO refactor when symmetrize will return a copiable graph
				final ImmutableGraph graph = new ArrayListMutableGraph(new ErdosRenyiGraph(size, p, 0, false)).immutableView();
				final ImmutableGraph transpose = Transform.transpose(graph);
				final LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(graph, transpose, NOPLogger.NOP_LOGGER);
				lsv.norm = Norm.L_1;
				lsv.salsa = true;
				lsv.stepUntil(new SpectralRanking.NormStoppingCriterion(1E-15));

				final double[] rank = Salsa.rank(graph, null);
				Norm.L_1.normalize(rank, 1);
				assertArrayEquals(lsv.rank, rank, 1E-2);
			}
		}
	}

	@Test
	public void testCliqueNobridgeCycle() {
		for(int p: new int[] { 10, 50, 100 }) {
			for(int k: new int[] { 10, 50, 100 }) {
				ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++) {
						if (i != j) mg.addArc(i, j);
					}
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				ImmutableGraph g = mg.immutableView();

				double[] rank = Salsa.rank(g, null);

				double[] expected = new double[rank.length];
				Arrays.fill(expected, 1);
				Norm.L_1.normalize(expected, 1);
				assertArrayEquals(expected, rank, 1E-10);
			}
		}
	}


	@Test
	public void testCliqueForwardbridgeCycle() {
		for(int p: new int[] { 10, 50, 100 }) {
			for(int k: new int[] { 10, 50, 100 }) {
				ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++) {
						if (i != j) mg.addArc(i, j);
					}
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k - 1, k);
				ImmutableGraph g = mg.immutableView();

				double[] rank = Salsa.rank(g, null);
				double[] expected = new double[rank.length];
				for(int i = k; i-- != 0;) expected[i] = (k + 1) * (k - 1) / (2. + k * (k - 1));
				expected[k] = (k + 1) * 2 / (2. + k * (k - 1));
				for(int d = 1; d < p; d++) expected[k + d] = 1;

				Norm.L_1.normalize(expected, 1);
				assertArrayEquals(expected, rank, 1E-10);
			}
		}
	}

	@Test
	public void testCliqueBackbridgeCycle() {
		for(int p: new int[] { 10, 50, 100 }) {
			for(int k: new int[] { 10, 50, 100 }) {
				ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++) {
						if (i != j) mg.addArc(i, j);
					}
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k, k - 1);
				ImmutableGraph g = mg.immutableView();

				double[] rank = Salsa.rank(g, null);

				double[] expected = new double[rank.length];
				for(int i = k - 1; i-- != 0;) expected[i] = (k + 1) * (k - 1) / (2. + k * (k - 1));
				expected[k - 1] = (k + 1) * k / (2. + k * (k - 1));
				expected[k] = 1;
				expected[k + 1] = (k + 1) / (2. + k * (k - 1));
				for(int d = 2; d < p; d++) expected[k + d] = 1;

				Norm.L_1.normalize(expected, 1);
				assertArrayEquals(expected, rank, 1E-10);
			}
		}
	}

	@Test
	public void testCliqueBibridgeCycle() {
		for(int p: new int[] { 10, 50, 100 }) {
			for(int k: new int[] { 10, 50, 100 }) {
				ArrayListMutableGraph mg = new ArrayListMutableGraph(p + k);
				for(int i = 0; i < k; i++)
					for(int j = 0; j < k; j++) {
						if (i != j) mg.addArc(i, j);
					}
				for(int i = 0; i < p; i++) mg.addArc(k + i, k + (i + 1) % p);
				mg.addArc(k, k - 1);
				mg.addArc(k - 1, k);
				ImmutableGraph g = mg.immutableView();

				double[] rank = Salsa.rank(g, null);

				double[] expected = new double[rank.length];
				for(int i = k - 1; i-- != 0;) expected[i] = (k + 2) * (k - 1) / (4. + k * (k - 1));
				expected[k - 1] = (k + 2) * k / (4. + k * (k - 1));
				expected[k] = (k + 2) * 2 / (4. + k * (k - 1));
				expected[k + 1] = (k + 2) / (4. + k * (k - 1));
				for(int d = 2; d < p; d++) expected[k + d] = 1;

				Norm.L_1.normalize(expected, 1);
				assertArrayEquals(expected, rank, 1E-10);
			}
		}
	}
}

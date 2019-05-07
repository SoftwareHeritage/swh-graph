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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.Util;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

/** Computes the visit order with respect to a breadth-first visit.
 *
 * @author Marco Rosa
 */


//RELEASE-STATUS: DIST

public class BFS {
	private final static Logger LOGGER = LoggerFactory.getLogger(BFS.class);

	/** Return the permutation induced by the visit order of a depth-first visit.
	 *
	 * @param graph a graph.
	 * @param startingNode the only starting node of the visit, or -1 for a complete visit.
	 * @param startPerm a permutation that will be used to shuffle successors.
	 * @return  the permutation induced by the visit order of a depth-first visit.
	 */
	public static int[] bfsperm(final ImmutableGraph graph, final int startingNode, final int[] startPerm) {
		final int n = graph.numNodes();

		final int[] visitOrder = new int[n];
		final int[] invStartPerm = Util.invertPermutation(startPerm, new int[n]);
		Arrays.fill(visitOrder, -1);
		final IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
		final LongArrayBitVector visited = LongArrayBitVector.ofLength(n);
		final ProgressLogger pl = new ProgressLogger(LOGGER);
		pl.expectedUpdates = n;
		pl.itemsName = "nodes";
		pl.start("Starting breadth-first visit...");
		Arrays.fill(visitOrder, -1);

		int pos = 0;

		for(int i = 0; i < n; i++) {
			final int start = i == 0 && startingNode != -1 ? startingNode : invStartPerm[i];
			if (visited.getBoolean(start)) continue;
			queue.enqueue(start);
			visited.set(start);

			int currentNode;
			final IntArrayList successors = new IntArrayList();

			while(! queue.isEmpty()) {
				currentNode = queue.dequeueInt();
				visitOrder[pos++] = currentNode;
				int degree = graph.outdegree(currentNode);
				final LazyIntIterator iterator = graph.successors(currentNode);

				successors.clear();
				while(degree-- != 0) {
					final int succ = iterator.nextInt();
					if (! visited.getBoolean(succ)) {
						successors.add(succ);
						visited.set(succ);
					}
				}

				final int[] randomSuccessors = successors.elements();
				IntArrays.quickSort(randomSuccessors, 0, successors.size(), (x, y) -> startPerm[x] - startPerm[y]);

				for(int j = successors.size(); j-- != 0;) queue.enqueue(randomSuccessors[j]);
				pl.update();
			}

			if (startingNode != -1) break;
		}

		pl.done();
		return visitOrder;
	}

	public static void main(final String[] args) throws JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(BFS.class.getName(), "Computes the permutation induced by a breadth-first visit.", new Parameter[] {
				new FlaggedOption("randomSeed", JSAP.LONG_PARSER, "0", JSAP.NOT_REQUIRED, 'r', "random-seed", "The random seed."),
				new FlaggedOption("initialNode", JSAP.INTEGER_PARSER, "-1", JSAP.NOT_REQUIRED, 'i', "initial-node", "The initial node of the visit. If specified, the visit will be performed only starting from the given node. The default performs a complete visit, iterating on all possible starting nodes."),
				new Switch("random", 'p', "Start from a random permutation."),
				new UnflaggedOption("graph", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the input graph"),
				new UnflaggedOption("perm", JSAP.STRING_PARSER, JSAP.REQUIRED, "The name of the output permutation"), });


		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final ImmutableGraph graph = ImmutableGraph.load(jsapResult.getString("graph"));

		final int n = graph.numNodes();
		final int[] startPerm = Util.identity(new int[n]);
		final long seed = jsapResult.getLong("randomSeed");
		final int initialnode = jsapResult.getInt("initialNode");
		if (jsapResult.getBoolean("random")) Collections.shuffle(IntArrayList.wrap(startPerm), new Random(seed));

		BinIO.storeInts(Util.invertPermutationInPlace(bfsperm(graph, initialnode, startPerm)), jsapResult.getString("perm"));
	}
}

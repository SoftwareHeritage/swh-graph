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
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongOpenHashBigSet;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.law.rank.PageRank;
import it.unimi.dsi.law.rank.PageRankParallelPowerSeries;
import it.unimi.dsi.law.rank.SpectralRanking;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ImmutableSubgraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;

//RELEASE-STATUS: DIST

/**
 * Removes nodes from a graph following a number of strategies.
 *
 * <p>This class has been used to perform the experiments described by Paolo Boldi, Marco Rosa, and
 * Sebastiano Vigna in &ldquo;Robustness of Social Networks: Comparative Results Based on Distance
 * Distributions&rdquo;, <i>Proceedings of the Third international Conference, SocInfo 2011</i>,
 * volume 6894 of Lecture Notes in Computer Science, pages 8&minus;21, Springer, 2011. The
 * implemented removal strategies are largest outdegree, label propagation, PageRank, PageRank on
 * the symmetrized graph, random and near-root (see the paper).
 *
 * <p>For each method and for each fraction of arcs two graph will be stored, with the following
 * basenames: <code><var>dest</var>-method-fraction</code> and
 * <code><var>dest</var>-method-fraction-tr</code>.
 *
 * <p>For each strategy and each fraction of arcs to be removed, nodes are removed from the original
 * graph (and its transpose) according to the total order specified by the strategy until the
 * specified fraction of arcs is removed.
 *
 * @author Marco Rosa
 * @author Sebastiano Vigna
 */

public class RemoveHubs {
	private final static Logger LOGGER = LoggerFactory.getLogger(RemoveHubs.class);

	private final static int[] reverse(final int[] perm) {
		final int length = perm.length;
		for(int i = length / 2; i-- != 0;) {
			final int t = perm[i];
			perm[i] = perm[length - i - 1];
			perm[length - i - 1] = t;
		}

		return perm;
	}

	protected static int[] store(final ImmutableGraph g, final ImmutableGraph gt, final double[] fraction, final int[] perm, final String dest, final String method) throws IOException {
		final int n = g.numNodes();
		final long m = g.numArcs();
		final IntSet sub = new IntOpenHashSet(Util.identity(n));
		final LongOpenHashBigSet removedArcs = new LongOpenHashBigSet();
		final int[] cut = new int[fraction.length];
		int count = 0;
		int i = n;

		BinIO.storeInts(perm, dest + "-" + method + ".perm");

		for (int j = 0; j < fraction.length; j++) {
			LOGGER.info("Storing fraction " + fraction[j] + "...");
			for (; i-- != 0 && count < (fraction[j] * m);) {
				final int x = perm[i];
				sub.remove(x);
				final LazyIntIterator successors = g.successors(x);
				for(int s; (s = successors.nextInt()) != -1;) {
					if (removedArcs.add((long)x << 32 | s)) count++;
				}
				final LazyIntIterator predecessors = gt.successors(x);
				for(int p; (p = predecessors.nextInt()) != -1;) {
					if (removedArcs.add((long)p << 32 | x)) count++;
				}
			}
			if (dest != null) {
				final int[] node = sub.toIntArray();
				Arrays.sort(node);
				BinIO.storeInts(node, dest + "-" + method + "-" + fraction[j] + ".subgraph");
				BVGraph.store(new ImmutableSubgraph(g, sub), dest + "-" + method + "-" + fraction[j], 4, 2, 0, -1, 0);
				BVGraph.store(new ImmutableSubgraph(gt, sub), dest + "-" + method + "-" + fraction[j] + "-t", 4, 2, 0, -1, 0);
			}
			cut[j] = i;
		}
		return cut;
	}

	protected static double[] pr(final ImmutableGraph gt) throws IOException {
		final PageRankParallelPowerSeries pr = new PageRankParallelPowerSeries(gt, 0, LOGGER);
		pr.alpha = .85;
		pr.stepUntil(PageRank.or(new SpectralRanking.NormStoppingCriterion(1E-14), new SpectralRanking.IterationNumberStoppingCriterion(PageRank.DEFAULT_MAX_ITER)));
		return pr.rank;
	}

	protected static int[] largestOutdegree(final ImmutableGraph g) {
		LOGGER.info("Removing by outdegree...");
		final int n = g.numNodes();

		final int[] degree = new int[n];
		for (final NodeIterator i = g.nodeIterator(); i.hasNext(); degree[i.nextInt()] = i.outdegree());

		final int[] perm = Util.identity(n);
		IntArrays.radixSortIndirect(perm, degree, false);

		return perm;
	}

	protected static int[] largestIndegree(final ImmutableGraph gt) {
		LOGGER.info("Removing by indegree...");
		final int n = gt.numNodes();

		final int[] degree = new int[n];
		for (final NodeIterator i = gt.nodeIterator(); i.hasNext(); degree[i.nextInt()] = i.outdegree());

		final int[] perm = Util.identity(n);
		IntArrays.radixSortIndirect(perm, degree, false);

		return perm;
	}

	protected static int[] labelPropagation(final ImmutableGraph symGraph) throws IOException {
		LOGGER.info("Removing by LP...");
		final int n = symGraph.numNodes();

		final LayeredLabelPropagation clustering = new LayeredLabelPropagation(symGraph, null, 0);
		final AtomicIntegerArray l = clustering.computeLabels(0);

		final int[] external = new int[n];
		for (final NodeIterator i = symGraph.nodeIterator(); i.hasNext();) {
			final int node = i.nextInt();
			final int deg = i.outdegree();
			final int[] succ = i.successorArray();
			final int label = l.get(node);
			for (int j = 0; j < deg; j++) {
				if (l.get(succ[j]) != label)
					external[node]++;
			}
		}

		final int[] perm = Util.identity(n);
		IntArrays.quickSort(perm, (x, y) -> {
			final int tmp = l.get(x) - l.get(y);
			if (tmp != 0) return tmp;
			else return external[y] - external[x];
		});

		int pos = 0;
		int currentLabel = l.get(perm[0]);
		for (int i = 0; i < n; i++) {
			final int node = perm[i];
			final int label = l.get(node);
			if (label != currentLabel) {
				pos = 0;
				currentLabel = l.get(node);
			}
			external[node] = ++pos;
		}

		IntArrays.radixSortIndirect(perm, external, false);
		return reverse(perm);
	}

	protected static int[] pageRank(final ImmutableGraph gt) throws IOException {
		LOGGER.info("Removing by PageRank...");
		return rank(gt, pr(gt));
	}

	protected static int[] rank(final ImmutableGraph g, final double[] rank) {
		final int n = g.numNodes();

		final int[] perm = Util.identity(n);
		IntArrays.quickSort(perm, (x, y) -> (int)Math.signum(rank[x] - rank[y]));

		return perm;
	}

	protected static int[] random(final ImmutableGraph g) {
		LOGGER.info("Removing randomly...");
		final int n = g.numNodes();
		final int[] perm = IntArrays.shuffle(Util.identity(n), new XoRoShiRo128PlusRandom(0));
		return perm;
	}

	protected static int[] symPageRank(final ImmutableGraph g, final ImmutableGraph gt) throws IOException {
		LOGGER.info("Removing by symmetric PageRank...");
		final int n = g.numNodes();

		final double[] rank = pr(Transform.symmetrize(g, gt, new ProgressLogger(LOGGER)));
		final int[] perm = Util.identity(n);
		IntArrays.quickSort(perm, (x, y) -> (int)Math.signum(rank[x] - rank[y]));

		return perm;
	}

	protected static int[] url(final ImmutableGraph g, final FastBufferedReader fastBufferedReader) throws IOException {
		LOGGER.info("Removing roots...");
		final int n = g.numNodes();

		final int[] slashes = new int[n];

		final MutableString s = new MutableString();
		for (int i = 0; i < n; i++) {
			fastBufferedReader.readLine(s);
			final char[] a = s.array();
			int t = 0;
			for (int j = s.length()-1; j-- != 0;) if (a[j] == '/') t++;
			slashes[i] = t;
		}

		final int[] perm = Util.identity(n);
		IntArrays.radixSortIndirect(perm, slashes, false);
		return reverse(perm);
	}

	public static void main(final String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(RemoveHubs.class.getName(), "Searches and removes hubs in a given graph.", new Parameter[] {
				new Switch("urls", 'u', "urls", "removes homepages (the algorithm expect that urls are given from standard input)."),
				new Switch("lp", 'l', "lp", "removes hubs by label propagation."),
				new Switch("degree", 'd', "degree", "removes hubs by largest outdegree."),
				new Switch("pr", 'p', "pr", "removes hubs by PageRank."),
				new FlaggedOption("rank", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'R', "rank", "removes hubs using an explicit rank."),
				new Switch("random", 'r', "random", "removes hubs randomly. "),
				new FlaggedOption("fraction", JSAP.STRING_PARSER, "0.05,0.1,0.15,0.2,0.3", JSAP.REQUIRED, 'f', "fraction", "A list of comma-separated values representing the fraction of arcs that must be removed."),
				new UnflaggedOption("g", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the graph."),
				new UnflaggedOption("gt", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the transposed graph."),
				new UnflaggedOption("sym", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the symmetrized graph."),
				new UnflaggedOption("dest", JSAP.STRING_PARSER, JSAP.REQUIRED, "The basename of the resulting subgraphs.") });

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted())
			System.exit(1);

		final ImmutableGraph g = ImmutableGraph.load(jsapResult.getString("g"));
		final ImmutableGraph gt = ImmutableGraph.load(jsapResult.getString("gt"));
		final ImmutableGraph sym = ImmutableGraph.load(jsapResult.getString("sym"));
		final String dest = jsapResult.getString("dest");

		final DoubleArrayList fraction = new DoubleArrayList();
		for (final String f : jsapResult.getString("fraction").split(","))
			fraction.add(Double.parseDouble(f));
		final double[] f = fraction.toDoubleArray();
		Arrays.sort(f);

		if (jsapResult.userSpecified("urls"))
			store(g, gt, f, url(g, new FastBufferedReader(new InputStreamReader(System.in, Charsets.ISO_8859_1))), dest, "urls");
		if (jsapResult.userSpecified("lp"))
			store(g, gt, f, labelPropagation(sym), dest, "lp");
		if (jsapResult.userSpecified("pr"))
			store(g, gt, f, pageRank(gt), dest, "pr");
		if (jsapResult.userSpecified("random"))
			store(g, gt, f, random(g), dest, "rnd");
		if (jsapResult.userSpecified("degree")) {
			store(g, gt, f, largestOutdegree(g), dest, "outdeg");
			store(g, gt, f, largestIndegree(gt), dest, "indeg");
		}
		if (jsapResult.userSpecified("rank")) {
			store(g, gt, f, rank(g, BinIO.loadDoubles(jsapResult.getString("rank"))), dest, "rank");
		}
	}
}

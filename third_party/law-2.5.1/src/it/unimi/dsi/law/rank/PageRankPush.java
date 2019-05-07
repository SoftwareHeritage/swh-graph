package it.unimi.dsi.law.rank;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.ConfigurationException;
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

/*
 * Copyright (C) 2010-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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
 */

import it.unimi.dsi.fastutil.booleans.BooleanArrays;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntPriorityQueue;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Properties;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

// RELEASE-STATUS: DIST

/** Computes strongly preferential PageRank for a preference vector concentrated on a node using the push algorithm.
 *
 *  <p>The <em>push algorithm</em> is an incremental way of computing PageRank that is particularly useful when the preference
 *  vector is nonzero on a single node, the root (i.e., the entire probability mass of the preference vector is in a single node).
 *  It was first proposed by Glen Jeh and Jennifer Widom in &ldquo;Scaling personalized web search&rdquo;, <i>Proc. of the
 *  Twelfth International World Wide Web Conference</i>, pages 271&minus;279, ACM Press, 2003.
 *
 *  <p>This implementation, in particular, computes strongly preferential PageRank for a single root node (i.e., dangling nodes donate all their rank to the root).
 *  Since often the set of nodes involved in the computation is a small fraction, we represent implicitly such nodes using their discovery order. As a result,
 *  the {@link PageRank#rank} vector does <em>not</em> contain the ranks, which can be recovered using the following code instead:
 *  <pre>
 *  double[] rank = new double[graph.numNodes()];
 *  for(int i = pageRank.node2Seen.size(); i-- != 0;) rank[pageRank.seen2Node[i]] = pageRank.rank[i] / pageRank.pNorm;
 *  </pre>
 *
 *  <p>In case you are interested in the <em>pseudorank</em>, instead, you should use
 *  <pre>
 *  double[] rank = new double[graph.numNodes()];
 *  for(int i = pageRank.node2Seen.size(); i-- != 0;) rank[pageRank.seen2Node[i]] = pageRank.rank[i] / (1 - pageRank.backToRoot);
 *  </pre>
 *
 *  <p>Details on the push algorithm have been given by Paolo Boldi and Sebastiano Vigna in
 *  &ldquo;<a href="http://vigna.di.unimi.it/papers.php#BoVPASR">The Push Algorithm for Spectral Ranking</a>&rdquo;.
 *  We implement both a priority-based update rule, and a simple FIFO update rule. Moreover, we implement <em>loop elimination</em> at the root, as described
 *  by Pavel Berkhin in &ldquo;Bookmark-coloring approach to personalized PageRank computing&rdquo;, <i>Internet Math.</i>, 3(1):41&minus;62, 2006.
 *
 * @author Sebastiano Vigna
 */
public class PageRankPush extends PageRank {
	private static final int INITIAL_SIZE = 16;

	private final static Logger LOGGER = LoggerFactory.getLogger(PageRankPush.class);

	/** A progress logger. */
	public final ProgressLogger progressLogger;
	/** The node where the preference vector is concentrated. */
	public int root;
	/** The threshold for stopping. */
	public double threshold = DEFAULT_THRESHOLD;
	/** The vector <var>r</var> (the r&ocirc;ole of <var>p</var> is covered by {@link #rank}). */
	public double[] residual;
	/** Whether we should use a {@link #fifoQueue} instead of an {@link #indirectQueue}. */
	private final boolean fifo;
	/** The update FIFO queue. You can choose at construction time whether to use this queue or {@link #indirectQueue}. */
	private IntPriorityQueue fifoQueue;
	/** The update priority queue. You can choose at construction time whether to use this queue or {@link #fifoQueue}. */
	private IntHeapIndirectPriorityQueue indirectQueue;
	/** The norm of the {@link #residual}. */
	private double rNorm;
	/** The stopping threshold multiplied by the number of nodes of the graph. */
	private double thresholdByNumNodes;
	/** Represents implicitly the set of elements in {@link #fifoQueue}, if the latter is used. */
	private boolean[] inQueue;

	/** The norm of the {@link #rank}. */
	public double pNorm;
	/** A map from nodes to the seen-order. */
	public Int2IntOpenHashMap node2Seen;
	/** A map from seen-order to nodes. */
	public int[] seen2Node;
	/** The amount of ranking going back to the root. */
	public double backToRoot;

	public final static class IntHeapIndirectPriorityQueue {
		/** The reference array. */
		public double[] refArray;
		/** The inversion set. */
		private int[] inv;
		/** The number of elements currently in the queue. */
		private int size;
		/** The heap. */
		private int[] heap;

		public IntHeapIndirectPriorityQueue() {
			this.inv = new int[INITIAL_SIZE];
			Arrays.fill(inv, -1);
			heap = new int[INITIAL_SIZE];
		}

		public static int upHeap(final double[] refArray, final int[] heap, final int[] inv, int i) {
			final int e = heap[i];
			final double E = refArray[e];
			int parent;
			while (i != 0 && (parent = (i - 1) / 2) >= 0) {
				if (refArray[heap[parent]] >= E) break;
				heap[i] = heap[parent];
				inv[heap[i]] = i;
				i = parent;
			}
			heap[i] = e;
			inv[e] = i;
			return i;
		}

		public static int downHeap(final double[] refArray, final int[] heap, final int[] inv, final int size, int i) {
			final int e = heap[i];
			final double E = refArray[e];
			int child;
			while ((child = 2 * i + 1) < size) {
				if (child + 1 < size && refArray[heap[child + 1]] > refArray[heap[child]]) child++;
				if (E >= refArray[heap[child]]) break;
				heap[i] = heap[child];
				inv[heap[i]] = i;
				i = child;
			}
			heap[i] = e;
			inv[e] = i;
			return i;
		}

		public void enqueue(final int x) {
			if (contains(x)) throw new IllegalArgumentException("Index " + x + " belongs to the queue");
			if (size == heap.length) heap = IntArrays.grow(heap, size + 1);

			if (x >= inv.length) {
				final int l = inv.length;
				inv = IntArrays.grow(inv, x + 1);
				Arrays.fill(inv, l, inv.length, -1);
			}

			inv[heap[size] = x] = size++;

			upHeap(refArray, heap, inv, size - 1);
		}

		public boolean contains(final int index) {
			return index < inv.length && inv[index] >= 0;
		}

		public int dequeue() {
			if (size == 0) throw new NoSuchElementException();
			final int result = heap[0];
			if (--size != 0) inv[heap[0] = heap[size]] = 0;
			inv[result] = -1;

			if (size != 0) downHeap(refArray, heap, inv, size, 0);
			return result;
		}

		public void changed() {
			downHeap(refArray, heap, inv, size, 0);
		}

		public boolean changed(final int index) {
			if (index >= inv.length) return false;
			final int pos = inv[index];
			if (pos < 0) return false;
			final int newPos = upHeap(refArray, heap, inv, pos);
			downHeap(refArray, heap, inv, size, newPos);
			return true;
		}

		public void clear() {
			size = 0;
			Arrays.fill(inv, -1);
		}

		public boolean isEmpty() {
			return size == 0;
		}
	}


	/** Creates a new instance.
	 *
	 * @param graph the graph on which to compute PageRank.
	 * @param logger a logger that will be passed to <code>super()</code>.
	 * @param fifo whether to use a FIFO queue instead of a priority queue to choose the next node to update.
	 */
	protected PageRankPush(final ImmutableGraph graph, final Logger logger, boolean fifo) {
		super(graph, logger);
		this.fifo = fifo;
		progressLogger = new ProgressLogger(logger);
	}


	/** Creates a new instance.
	 *
	 * @param graph the graph on which to compute PageRank.
	 * @param fifo whether to use a FIFO queue instead of a priority queue to choose the next node to update.
	 */
	public PageRankPush(final ImmutableGraph graph, final boolean fifo) {
		this(graph, LOGGER, fifo);
	}

	@Override
	public void clear() {
		fifoQueue = null;
		indirectQueue = null;
		residual = null;
		rank = null;
		seen2Node = null;
		node2Seen = null;
		inQueue = null;
	}

	@Override
	public void init() throws IOException {
		// We do not call super(), as we do not want to initialize rank.
		int prevSize = -1;
		if (node2Seen == null) (node2Seen = new Int2IntOpenHashMap()).defaultReturnValue(-1);
		else {
			prevSize = node2Seen.size();
			node2Seen.clear();
		}

		if (seen2Node == null) seen2Node = new int[INITIAL_SIZE];

		if (residual == null) residual = new double[INITIAL_SIZE];
		else Arrays.fill(residual, 0, prevSize, 0);

		if (rank == null) rank = new double[INITIAL_SIZE];
		else Arrays.fill(rank, 0, prevSize, 0);

		if (fifo) {
			if (fifoQueue == null) fifoQueue = new IntArrayFIFOQueue();
			else fifoQueue.clear();
			if (inQueue == null) inQueue = new boolean[INITIAL_SIZE];
			else Arrays.fill(inQueue, 0, prevSize, false);
		}
		else {
			if (indirectQueue == null) indirectQueue = new IntHeapIndirectPriorityQueue();
			else indirectQueue.clear();
		}

		logger.info("Initialising...");
		logger.info("alpha = " + alpha);

		if (fifoQueue == null) indirectQueue.refArray = residual;

		pNorm = 0;
		rNorm = 1;
		backToRoot = 0;
		thresholdByNumNodes = threshold / n;

		node2Seen.put(root, 0);
		seen2Node[0] = root;
		if (fifo) fifoQueue.enqueue(0);
		else indirectQueue.enqueue(0);
		residual[0] = 1;

		progressLogger.itemsName = "pushes";
		progressLogger.start("Computing...");
	}


	public void step() {
		final boolean fifo = this.fifo;
		int curr = fifo ? fifoQueue.dequeueInt() : indirectQueue.dequeue();
		if (fifo) inQueue[curr] = false;
		double residualCurr = residual[curr];
		rank[curr] += (1 - alpha) * residualCurr;
		pNorm += (1 - alpha) * residualCurr;

		final int node = seen2Node[curr];
		final int d = graph.outdegree(node);
		final LazyIntIterator successors = graph.successors(node);

		final double alphaByd = alpha / d;
		int nonRoot = d;

		for(int i = d; i-- != 0;) {
			final int s = successors.nextInt();
			if (s == root) {
				// Berkhin's loop elimination
				backToRoot += alphaByd * residualCurr;
				nonRoot--;
				continue;
			}

			int u = node2Seen.get(s);
			if (u == -1) {
				node2Seen.put(s, u = node2Seen.size());
				if (u >= residual.length) {
					rank = DoubleArrays.grow(rank, u + 1);
					residual = DoubleArrays.grow(residual, u + 1);
					seen2Node = IntArrays.grow(seen2Node, u + 1);
					if (fifo) inQueue = BooleanArrays.grow(inQueue, u + 1);
					else indirectQueue.refArray = residual;
				}
				seen2Node[u] = s;
			}
			else if (u == curr) throw new IllegalArgumentException("The graph must be loopless");

			residual[u] += alphaByd * residualCurr;

			if (fifo) {
				if ((1 - backToRoot) * residual[u] >= thresholdByNumNodes * pNorm && ! inQueue[u]) {
					inQueue[u] = true;
					fifoQueue.enqueue(u);
				}
			}
			else {
				if (indirectQueue.contains(u)) indirectQueue.changed(u);
				else if (residual[u] * (1 - backToRoot) >= thresholdByNumNodes * pNorm) indirectQueue.enqueue(u);
			}
		}

		residual[curr] = 0;
		rNorm -= residualCurr;

		if (d != 0) rNorm += (alphaByd * nonRoot) * residualCurr;

		progressLogger.lightUpdate();
	}

	public boolean queueIsEmpty() {
		return fifo ? fifoQueue.isEmpty() : indirectQueue.isEmpty();
	}

	public static class EmptyQueueStoppingCritertion implements StoppingCriterion {
		@Override
		public boolean shouldStop(SpectralRanking p) {
			return ((PageRankPush)p).queueIsEmpty();
		}
	}

	public static class L1NormStoppingCritertion implements StoppingCriterion {
		@Override
		public boolean shouldStop(SpectralRanking p) {
			final PageRankPush pracl = (PageRankPush)p;
			return pracl.queueIsEmpty() || (1 - pracl.backToRoot) * pracl.rNorm <= pracl.pNorm * pracl.threshold;
		}
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException {

		final SimpleJSAP jsap = new SimpleJSAP(PageRankPush.class.getName(),
			"Computes strongly preferential PageRank for a preference vector concentrated on a node using the push algorithm.",
			new Parameter[] {
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, Double.toString(PageRank.DEFAULT_ALPHA), JSAP.NOT_REQUIRED, 'a', "alpha", "The damping factor."),
			new FlaggedOption("root", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'r', "root", "The node where the preference vector is concentrated."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "The L1-norm threshold."),
			new Switch("l1Norm", '1', "l1-norm", "Use the relativized L1-norm as stopping criterion."),
			new Switch("fifo", 'f', "FIFO", "Whether to use a FIFO queue instead of a priority queue to choose the next node to update."),
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename where the results will be stored. <rankBasename>.properties will contain the parameter values used in the computation. <rankBasename>.ranks will contain the ranks (doubles in binary form).")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String graphBasename = jsapResult.getString("graphBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final double threshold = jsapResult.getDouble("threshold");

		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");
		ImmutableGraph graph = null;

		graph = ImmutableGraph.load(graphBasename, progressLogger);
		if (jsapResult.userSpecified("expand")) graph = new ArrayListMutableGraph(graph).immutableView();

		PageRankPush pr = new PageRankPush(graph, LOGGER, jsapResult.userSpecified("fifo"));
		pr.alpha = jsapResult.getDouble("alpha");
		pr.root = jsapResult.getInt("root");
		pr.threshold = threshold;

		if (jsapResult.userSpecified("l1Norm")) pr.stepUntil(new L1NormStoppingCritertion());
		else pr.stepUntil(new EmptyQueueStoppingCritertion());
		pr.progressLogger.done();

		System.err.print("Saving ranks...");
		double[] rank = new double[graph.numNodes()];
		for(int i = pr.node2Seen.size(); i-- != 0;) rank[pr.seen2Node[i]] = pr.rank[i] / pr.pNorm;

		BinIO.storeDoubles(rank, rankBasename +".ranks");
		Properties prop = new Properties();
		prop.setProperty("rank.alpha", Double.toString(pr.alpha));
		prop.setProperty("graph.fileName", graphBasename);
		prop.setProperty("root", pr.root);
		prop.setProperty("fifo", jsapResult.userSpecified("fifo"));
		prop.save(rankBasename + ".properties");

		System.err.println(" done.");
	}
}

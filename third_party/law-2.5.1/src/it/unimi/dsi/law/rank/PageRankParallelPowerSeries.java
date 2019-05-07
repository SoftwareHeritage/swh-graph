package it.unimi.dsi.law.rank;

import java.io.IOException;
import java.util.BitSet;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableDouble;
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
 * Copyright (C) 2011-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.law.util.KahanSummation;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

// RELEASE-STATUS: DIST

/** Computes PageRank using a parallel (multicore) implementation of the {@linkplain PageRankPowerSeries power-series method}, which runs
 * the power method starting from the preference vector, thus evaluating the truncated PageRank power series (see {@link PageRankPowerSeries}).
 *
 * <p>Note that the {@link #step()} method is not available: due to the need for some synchronization logic, only {@link #stepUntil(StoppingCriterion)}
 * is available.
 *
 * <p><em>This class exists for debugging and comparison purposes only</em>. The class of choice for computing PageRank is {@link PageRankParallelGaussSeidel}.
 *
 * <p><strong>Warning</strong>: Since we need to enumerate the <em>predecessors</em> a node,
 * you must pass to the {@linkplain #PageRankParallelPowerSeries(ImmutableGraph, int, Logger) constructor} the <strong>transpose</strong>
 * of the graph.
 *
 * @see PageRankPowerSeries
 * @see PageRank
 * @see SpectralRanking
 *
 * @author Sebastiano Vigna
 */

public class PageRankParallelPowerSeries extends PageRank {
	private final static Logger LOGGER = LoggerFactory.getLogger(PageRankParallelPowerSeries.class);

	/** A progress logger monitoring each iteration. */
	private final ProgressLogger progressLogger;
	/** A progress logger monitoring the iterations. */
	private final ProgressLogger iterationLogger;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node to be picked. */
	private final AtomicLong nextNode;
	/** The rank lost through dangling nodes. */
	private final MutableDouble danglingRankAccumulator;
	/** The outdegree of each node. */
	private int[] outdegree;
	/** If true, the computation is over. */
	private volatile boolean completed;
	/** The barrier used to synchronize threads. */
	private volatile CyclicBarrier barrier;
	/** Keeps track of problems in threads. */
	private volatile Throwable threadThrowable;
	/** The rank vector after the last iteration (only meaningful after at least one step). */
	public double[] previousRank;

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute PageRank.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public PageRankParallelPowerSeries(final ImmutableGraph transpose, final int requestedThreads, final Logger logger) {
		super(transpose, logger);
		progressLogger = new ProgressLogger(logger, "nodes");
		iterationLogger = new ProgressLogger(logger, "iterations");
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
		nextNode = new AtomicLong();
		danglingRankAccumulator = new MutableDouble();
	}

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute PageRank.
	 */
	public PageRankParallelPowerSeries(final ImmutableGraph transpose) {
		this(transpose, 0, LOGGER);
	}

	@Override
	public void init() throws IOException {
		super.init();

		// Creates the arrays, if necessary
		if (previousRank == null) previousRank = new double[n];

		if (outdegree == null) {
			// We allocate and compute the outdegree vector.
			outdegree = new int[n];
			// TODO: refactor using .outdegrees().
			progressLogger.expectedUpdates = n;
			progressLogger.start("Computing outdegrees...");

			final NodeIterator nodeIterator = graph.nodeIterator();
			for(int i = n; i-- != 0;) {
				nodeIterator.nextInt();
				final int[] pred = nodeIterator.successorArray();
				for (int d = nodeIterator.outdegree(); d-- != 0;) outdegree[pred[d]]++;
				progressLogger.lightUpdate();
			}

			progressLogger.done();
		}

		danglingRankAccumulator.setValue(0);
		completed = false;
		logger.info("Completed.");
		iterationLogger.start();
	}

	private final class IterationThread extends Thread {
		private static final int GRANULARITY = 10000;

		public void run() {
			try {
				// We cache frequently used fields.
				final ImmutableGraph graph = PageRankParallelPowerSeries.this.graph.copy();
				final BitSet buckets = PageRankParallelPowerSeries.this.buckets;
				final int[] outdegree = PageRankParallelPowerSeries.this.outdegree;
				final int n = PageRankParallelPowerSeries.this.n;
				final double alpha = PageRankParallelPowerSeries.this.alpha;
				final DoubleList preference = PageRankParallelPowerSeries.this.preference;
				final KahanSummation s = new KahanSummation();

				for(;;) {
					barrier.await();
					if (completed) return;
					final double[] oldRank = rank, newRank = previousRank;

					for(;;) {
						// Try to get another piece of work.
						final long start = nextNode.getAndAdd(GRANULARITY);
						if (start >= n) {
							nextNode.getAndAdd(-GRANULARITY);
							break;
						}

						final int end = (int)(Math.min(n, start + GRANULARITY));

						// for each node, enumerate predecessors and compute an updated value
						double accum = 0.0;
						final NodeIterator nodeIterator = graph.nodeIterator((int)start);

						for(int i = (int)start; i < end; i++) {
							nodeIterator.nextInt();
							if (outdegree[i] == 0 || buckets != null && buckets.get(i)) accum += oldRank[i];
							int indegree = nodeIterator.outdegree();
							s.reset();
							if (indegree != 0) {
								final int[] pred = nodeIterator.successorArray();
								if (buckets == null) while (indegree-- != 0) s.add(oldRank[pred[indegree]] / outdegree[pred[indegree]]);
								else while (indegree-- != 0) if (! buckets.get(pred[indegree])) s.add(oldRank[pred[indegree]] / outdegree[pred[indegree]]);
							}

							if (preference != null) newRank[i] = alpha * s.value() + (1 - alpha) * preference.getDouble(i);
							else newRank[i] = alpha * s.value() + (1 - alpha) / n;
						}

						synchronized (progressLogger) {
							progressLogger.update(end - start);
						}

						synchronized (danglingRankAccumulator) {
							danglingRankAccumulator.add(accum);
						}
					}
				}
			}
			catch(Throwable t) {
				threadThrowable = t;
			}
		}
	}

	@Override
	public void step() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void stepUntil(final StoppingCriterion stoppingCriterion) throws IOException {
		init();
		final IterationThread[] thread = new IterationThread[numberOfThreads];
		for(int i = thread.length; i-- != 0;) thread[i] = new IterationThread();

		barrier = new CyclicBarrier(numberOfThreads, new Runnable() {
			@Override
			public void run() {
				if (iteration > 0) {
					progressLogger.done();

					final double t[] = rank;
					rank = previousRank;
					previousRank = t;

					final double adjustment = danglingNodeDistribution == null ? alpha * danglingRankAccumulator.doubleValue() / n : alpha * danglingRankAccumulator.doubleValue();
					if (preference != null)
						if (danglingNodeDistribution == null)
							for(int i = n; i-- != 0;) rank[i] += adjustment;
						else
							for(int i = n; i-- != 0;) rank[i] += adjustment * danglingNodeDistribution.getDouble(i);
					else
						if (danglingNodeDistribution == null)
							for(int i = n; i-- != 0;) rank[i] += adjustment;
						else
							for(int i = n; i-- != 0;) rank[i] += adjustment * danglingNodeDistribution.getDouble(i);

					iterationLogger.setAndDisplay(iteration);

					if (stoppingCriterion.shouldStop(PageRankParallelPowerSeries.this)) {
						completed = true;
						return;
					}
				}

				danglingRankAccumulator.setValue(0);
				nextNode.set(0);
				progressLogger.expectedUpdates = n;
				progressLogger.start("Iteration " + iteration++ + "...");
			}
		}
		);

		for(int i = thread.length; i-- != 0;) thread[i].start();
		for(int i = thread.length; i-- != 0;)
			try {
				thread[i].join();
			}
			catch (InterruptedException e) {
				throw new RuntimeException(e);
			}

		if (threadThrowable != null) throw new RuntimeException(threadThrowable);
		if (progressLogger != null) progressLogger.done();

		iterationLogger.done();
	}


	@Override
	public double normDelta() {
		return Norm.L_1.compute(rank, previousRank) * alpha / (1 - alpha);
	}

	@Override
	public void clear() {
		super.clear();
		previousRank = null;
		outdegree = null;
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException, ClassNotFoundException {

		SimpleJSAP jsap = new SimpleJSAP(PageRankParallelPowerSeries.class.getName(), "Computes PageRank of a graph, given its transpose, using a parallel implementation of the power-series method."
				+ " The file <rankBasename>.properties stores metadata about the computation, whereas the file <rankBasename>.ranks stores the result as a sequence of doubles in DataInput format.",
			new Parameter[] {
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, Double.toString(PageRank.DEFAULT_ALPHA), JSAP.NOT_REQUIRED, 'a', "alpha", "Damping factor."),
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "Threshold to determine whether to stop."),
			new FlaggedOption("preferenceVector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "preference-vector", "A preference vector stored as a vector of binary doubles."),
			new FlaggedOption("preferenceObject", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'P', "preference-object", "A preference vector stored as a serialised DoubleList."),
			new FlaggedOption("buckets", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "buckets", "The buckets of the graph; if supplied, buckets will be treated as dangling nodes."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
			new Switch("strongly", 'S', "strongly", "use the preference vector to redistribute the dangling rank."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("transposeBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final boolean strongly = jsapResult.getBoolean("strongly", false);
		final String graphBasename = jsapResult.getString("transposeBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final String buckets = jsapResult.getString("buckets");
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");

		ImmutableGraph graph = mapped? ImmutableGraph.loadMapped(graphBasename, progressLogger) : ImmutableGraph.load(graphBasename, progressLogger);

		DoubleList preference = null;
		String preferenceFilename = null;
		if (jsapResult.userSpecified("preferenceVector"))
			preference = DoubleArrayList.wrap(BinIO.loadDoubles(preferenceFilename = jsapResult.getString("preferenceVector")));

		if (jsapResult.userSpecified("preferenceObject")) {
			if (jsapResult.userSpecified("preferenceVector")) throw new IllegalArgumentException("You cannot specify twice the preference vector");
			preference = (DoubleList)BinIO.loadObject(preferenceFilename = jsapResult.getString("preferenceObject"));
		}

		if (strongly && preference == null) throw new IllegalArgumentException("The 'strongly' option requires a preference vector");

		if (jsapResult.userSpecified("expand")) graph = new ArrayListMutableGraph(graph).immutableView();

		final PageRankParallelPowerSeries pr = new PageRankParallelPowerSeries(graph, threads, LOGGER);
		pr.alpha = jsapResult.getDouble("alpha");
		pr.preference = preference;
		pr.buckets = (BitSet)(buckets == null ? null : BinIO.loadObject(buckets));
		pr.stronglyPreferential = strongly;

		pr.stepUntil(or(new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(pr.rank, rankBasename + ".ranks");
		pr.buildProperties(graphBasename, preferenceFilename, null).save(rankBasename + ".properties");
	}
}

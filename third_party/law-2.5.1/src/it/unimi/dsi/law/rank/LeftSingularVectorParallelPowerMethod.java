package it.unimi.dsi.law.rank;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

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
 * Copyright (C) 2004-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.law.util.KahanSummation;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Properties;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

// RELEASE-STATUS: DIST

/** Computes the left singular vector of a graph using a parallel implementation of the power method.
 *
 * <p>This class computes iteratively and in parallel an approximation of the left singular vector of a graph
 * with adjacency matrix <var>M</var>, that is, the dominant eigenvector of <var>M</var><var>M</var><sup>T</sup>.
 * Note that the {@link #step()} method is not available: due to the need for some synchronization logic, only {@link #stepUntil(StoppingCriterion)}
 * is available.
 *
 * <p>This class can be run using {@link SpectralRanking.NormStoppingCriterion} that stops, as usual, when {@link #normDelta()},
 * which returns the {@linkplain #norm} of the difference between the two last approximations,
 * is below a specified threshold.
 *
 * <p>It is <strong>strongly suggested</strong> that you apply a <em>{@linkplain #shift}</em> (e.g., -1). A negative shift
 * guarantees convergence even in the presence of eigenvalues of maximum modulus that are not real positive (see a numerical
 * linear algebra textbook for details).
 *
 * @author Sebastiano Vigna
 */

public class LeftSingularVectorParallelPowerMethod extends SpectralRanking {
	private final static Logger LOGGER = LoggerFactory.getLogger(LeftSingularVectorParallelPowerMethod.class);

	/** The transpose of the current graph. */
	private ImmutableGraph transpose;
	/** A progress logger monitoring each iteration. */
	private final ProgressLogger progressLogger;
	/** A progress logger monitoring the iterations. */
	private final ProgressLogger iterationLogger;
	/** The norm. */
	public Norm norm = DEFAULT_NORM;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node to be picked. */
	private final AtomicLong nextNode;
	/** If true, the computation is over. */
	private volatile boolean completed;
	/** If true, the computation was interrupted by the detection of an error condition. */
	private volatile boolean interrupted;
	/** If true, we are performing the first half of a round (i.e., the first multiplication). */
	private volatile boolean roundFirstHalf;
	/** The barrier used to synchronize threads. */
	private volatile CyclicBarrier barrier;
	/** Keeps track of problems in  threads. */
	private volatile Throwable threadThrowable;
	/** Compute the SALSA score (only for historical and testing reasons: please use the {@link Salsa} class instead). */
	public boolean salsa;
	/** A shift. */
	public double shift;
	/** The rank vector obtained after the first half of a round. */
	public double[] intermediateRank;
	/** The rank vector at the end of the last round. */
	public double[] previousRank;



	/** Creates a new instance.
	 *
	 * @param graph the graph on which the computation must be performed.
	 * @param transpose the tranpose of the graph on which the computation must be performed.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public LeftSingularVectorParallelPowerMethod(final ImmutableGraph graph, final ImmutableGraph transpose, final int requestedThreads, final Logger logger) {
		super(graph, logger);
		this.transpose = transpose;
		progressLogger = new ProgressLogger(logger, "nodes");
		iterationLogger = new ProgressLogger(logger, "iterations");
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
		nextNode = new AtomicLong();
	}

	/** Creates a new instance.
	 *
	 * @param graph the graph on which the computation must be performed.
	 * @param transpose the tranpose of the graph on which the computation must be performed.
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public LeftSingularVectorParallelPowerMethod(final ImmutableGraph graph, final ImmutableGraph transpose, final Logger logger) {
			this(graph, transpose, 0, logger);
	}

	/** Creates a new instance.
	 *
	 * @param graph the graph on which the computation must be performed.
	 * @param transpose the tranpose of the graph on which the computation must be performed.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 */
	public LeftSingularVectorParallelPowerMethod(final ImmutableGraph graph, final ImmutableGraph transpose, final int requestedThreads) {
			this(graph, transpose, requestedThreads, LOGGER);
	}

	/** Creates a new instance.
	 *
	 * @param graph the graph on which the computation must be performed.
	 * @param transpose the tranpose of the graph on which the computation must be performed.
	 */
	public LeftSingularVectorParallelPowerMethod(final ImmutableGraph graph, final ImmutableGraph transpose) {
		this(graph, transpose, 0);
	}

	@Override
	public void init() throws IOException {
		super.init();

		logger.info("Norm: " + norm);
		logger.info("Shift: " + shift);
		logger.info("SALSA: " + salsa);

		interrupted = completed = false;
		roundFirstHalf = true;
		// Creates the arrays, if necessary
		if (previousRank == null) previousRank = new double[n];
		if (intermediateRank == null) intermediateRank = new double[n];

		// Check the preference vector
		logger.info("Using the uniform preference vector");
		Arrays.fill(rank, 1);
		norm.normalize(rank, 1);

		logger.info("Completed.");
		iterationLogger.start();
	}

	private final class IterationThread extends Thread {
		private static final int GRANULARITY = 10000;

		public void run() {
			try {
				// We cache frequently used fields.
				final ImmutableGraph graph = LeftSingularVectorParallelPowerMethod.this.graph.copy();
				final ImmutableGraph transpose = LeftSingularVectorParallelPowerMethod.this.transpose.copy();
				final int n = LeftSingularVectorParallelPowerMethod.this.n;
				final double shift = LeftSingularVectorParallelPowerMethod.this.shift;
				final boolean salsa = LeftSingularVectorParallelPowerMethod.this.salsa;
				final KahanSummation s = new KahanSummation();

				for(;;) {
					barrier.await();
					if (completed) return;

					final boolean roundFirstHalf = LeftSingularVectorParallelPowerMethod.this.roundFirstHalf;
					final ImmutableGraph graphToBeUsed = roundFirstHalf? graph : transpose;
					final ImmutableGraph otherGraph = roundFirstHalf? transpose : graph;
					final double[] rank = LeftSingularVectorParallelPowerMethod.this.rank;
					final double[] oldRank = roundFirstHalf? rank : intermediateRank;
					final double[] newRank = roundFirstHalf? intermediateRank : previousRank;
					final boolean doShift = ! roundFirstHalf && shift != 0;

					for(;;) {
						// Try to get another piece of work.
						final long start = nextNode.getAndAdd(GRANULARITY);
						if (start >= n) {
							nextNode.getAndAdd(-GRANULARITY);
							break;
						}

						final int end = (int)(Math.min(n, start + GRANULARITY));

						// for each node, enumerate predecessors and compute an updated value
						final NodeIterator nodeIterator = graphToBeUsed.nodeIterator((int)start);

						for(int i = (int)start; i < end; i++) {
							nodeIterator.nextInt();
							int indegree = nodeIterator.outdegree();
							s.reset();

							if (indegree != 0) {
								final int[] pred = nodeIterator.successorArray();
								if (salsa) while (indegree-- != 0) {
									final int p = pred[indegree];
									s.add(oldRank[p] / otherGraph.outdegree(p));
								}
								else while (indegree-- != 0) s.add(oldRank[pred[indegree]]);
							}

							newRank[i] = s.value();
							if (doShift) newRank[i] -= shift * rank[i];
						}

						synchronized (progressLogger) {
							progressLogger.update(end - start);
						}
					}

					synchronized(LeftSingularVectorParallelPowerMethod.this) {
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

					if (!roundFirstHalf) {
						final double t[] = rank;
						rank = previousRank;
						previousRank = t;

						norm.normalize(rank, 1); // Normalize
						iterationLogger.setAndDisplay(iteration);

						if (stoppingCriterion.shouldStop(LeftSingularVectorParallelPowerMethod.this)) {
							completed = true;
							return;
						}
					}
					roundFirstHalf = !roundFirstHalf;
				}

				nextNode.set(0);
				progressLogger.expectedUpdates = n;
				progressLogger.start("Iteration " + (iteration++) + "...");
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
		if (interrupted) throw new RuntimeException("Computation interrupted.");
		if (progressLogger != null) progressLogger.done();

		iterationLogger.done();
	}

	@Override
	public double normDelta() {
		return norm.compute(rank, previousRank);
	}

	@Override
	public void clear() {
		super.clear();
		previousRank = null;
		intermediateRank = null;
	}

	/**
	 * Returns a Properties object that contains all the parameters used by the computation.
	 *
	 * @param graphBasename file name of the graph
	 * @return a properties object that represent all the parameters used to calculate the rank.
	 */
	public Properties buildProperties(String graphBasename) {
		final Properties prop = super.buildProperties(graphBasename);
		prop.setProperty("norm", norm);
		prop.setProperty("salsa", salsa);
		prop.setProperty("shift", shift);
		return prop;
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException {

		SimpleJSAP jsap = new SimpleJSAP(LeftSingularVectorParallelPowerMethod.class.getName(), "Computes the left singular eigenvector a graph using a parallel implementation." +
				" The file <rankBasename>.properties stores metadata about the computation, whereas the file <rankBasename>.ranks stores the result as a sequence of doubles in DataInput format.",
			new Parameter[] {
			new FlaggedOption("norm", JSAP.STRING_PARSER, DEFAULT_NORM.toString(), JSAP.NOT_REQUIRED, 'n', "norm", "Norm type. Possible values: " + Arrays.toString(Norm.values())),
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "Threshold to determine whether to stop (not use for suitable vectors)."),
			new FlaggedOption("shift", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shift", "A shift for the power method."),
			new Switch("salsa", 'S', "salsa", "Compute SALSA (only for historical and testing reasons: please use the Salsa class instead)."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("transposeBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final boolean salsa = jsapResult.getBoolean("salsa", false);
		final String graphBasename = jsapResult.getString("graphBasename");
		final String transposeBasename = jsapResult.getString("transposeBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final String norm = jsapResult.getString("norm");
		final double shift = jsapResult.getDouble("shift", 0);
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");

		final ImmutableGraph graph = mapped? ImmutableGraph.loadMapped(graphBasename, progressLogger) : ImmutableGraph.load(graphBasename, progressLogger);
		final ImmutableGraph transpose = mapped? ImmutableGraph.loadMapped(transposeBasename, progressLogger) : ImmutableGraph.load(transposeBasename, progressLogger);

		LeftSingularVectorParallelPowerMethod lsv = new LeftSingularVectorParallelPowerMethod(graph, transpose, threads, LOGGER);
		lsv.norm = Norm.valueOf(norm);
		lsv.salsa = salsa;
		lsv.shift = shift;

		lsv.stepUntil(or(new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(lsv.rank, rankBasename + ".ranks");
		lsv.buildProperties(graphBasename).save(rankBasename + ".properties");
	}
}

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
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;

// RELEASE-STATUS: DIST

/** Computes the left dominant eigenvalue and eigenvector of a graph using a parallel implementation of the power method.
 * At the end of the computation, {@link #lambda} will contain an approximation of the dominant eigenvalue.
 *
 * <ul>
 * <li>If the {@linkplain #markovian Markovian flag} has been set, the rows of the adjacency matrix will be &#x2113;<sub>1</sub>-normalized.
 *
 * <li>{@link #normDelta()} returns the difference in {@linkplain #norm} between the previous approximation of the dominant eigenvector
 * and the product of the previous approximation by the graph, divided by the current estimate of the dominant eigenvalue obtained
 * by Rayleigh quotients.
 *
 * <li>The {@link #step()} method is not available: due to the need for some synchronization logic, only {@link #stepUntil(StoppingCriterion)}
 * is available.
 *
 * <li>The computed eigenvector will be a unit vector in the specified
 * {@linkplain #norm norm}. In particular, in the Markovian case if you use a norm different from {@link Norm#L_1} the dominant eigenvector will need to be
 * &#x2113;<sub>1</sub>-normalized to be a distribution.
 *
 * <li>It is <strong>strongly suggested</strong> that you apply a <em>{@linkplain #shift}</em> (e.g., -1). A negative shift
 * guarantees convergence even in the presence of eigenvalues of maximum modulus that are not real positive (see a numerical
 * linear algebra textbook for details).
 *
 * </ul>
 * @see SpectralRanking
 *
 * @author Sebastiano Vigna
 */

public class DominantEigenvectorParallelPowerMethod extends SpectralRanking {
	private final static Logger LOGGER = LoggerFactory.getLogger(DominantEigenvectorParallelPowerMethod.class);

	/** The default norm ({@link Norm#L_2}). Works well with Rayleigh-quotient estimation of the dominant eigenvalue. */
	public final static Norm DEFAULT_DOMINANT_EIGENVECTOR_NORM = Norm.L_2;

	/** A progress logger monitoring each iteration. */
	private final ProgressLogger progressLogger;
	/** A progress logger monitoring the iterations. */
	private final ProgressLogger iterationLogger;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node to be picked. */
	private final AtomicLong nextNode;
	/** Accumulates the numerator of the Rayleigh quotient. */
	private final KahanSummation rayleighQuotientNumerator;
	/** Accumulates the denominator of the Rayleigh quotient. */
	private final KahanSummation rayleighQuotientDenominator;
	/** If true, the computation is over. */
	private volatile boolean completed;
	/** The barrier used to synchronize threads. */
	private volatile CyclicBarrier barrier;
	/** Keeps track of problems in  threads. */
	private volatile Throwable threadThrowable;
	/** The outdegree of each node ({@code null} if {@link #markovian} is false). */
	private int[] outdegree;
	/** The rank vector after the last iteration (only meaningful after at least one step). */
	public double[] previousRank;
	/** if true, the matrix will be stocasticized. */
	public boolean markovian;
	/** A shift. */
	public double shift;
	/** The dominant eigenvalue. */
	public double lambda;
	/** The norm. */
	public Norm norm = DEFAULT_DOMINANT_EIGENVECTOR_NORM;

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public DominantEigenvectorParallelPowerMethod(final ImmutableGraph transpose, final int requestedThreads, final Logger logger) {
		super(transpose, logger);
		progressLogger = new ProgressLogger(logger, "nodes");
		iterationLogger = new ProgressLogger(logger, "iterations");
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
		nextNode = new AtomicLong();
		rayleighQuotientNumerator = new KahanSummation();
		rayleighQuotientDenominator = new KahanSummation();
	}

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph.
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public DominantEigenvectorParallelPowerMethod(final ImmutableGraph transpose, final Logger logger) {
		this(transpose, 0, logger);
	}

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph.
	 */
	public DominantEigenvectorParallelPowerMethod(final ImmutableGraph transpose) {
		this(transpose, LOGGER);
	}

	@Override
	public void init() throws IOException {
		super.init();
		completed = false;
		logger.info("Norm: " + norm);
		logger.info("Shift: " + shift);
		logger.info("Markovian: " + markovian);

		if (markovian && outdegree == null) {
			// Compute outdegrees
			outdegree = new int[n];
			final NodeIterator nodeIterator = graph.nodeIterator();
			for(int i = n; i-- != 0;) {
				nodeIterator.nextInt();
				LazyIntIterator successors = nodeIterator.successors();
				for(int s; (s = successors.nextInt()) != -1;) outdegree[s]++;
			}
		}
		else outdegree = null;

		// Creates the arrays, if necessary
		if (previousRank == null) previousRank = new double[n];
		Arrays.fill(rank, 1);
		norm.normalize(rank, 1);

		rayleighQuotientNumerator.reset();
		rayleighQuotientDenominator.reset();

		logger.info("Completed.");
		iterationLogger.start();
	}

	private final class IterationThread extends Thread {
		private static final int GRANULARITY = 10000;

		public void run() {
			try {
				// We cache frequently used fields.
				final ImmutableGraph graph = DominantEigenvectorParallelPowerMethod.this.graph.copy();
				final int n = DominantEigenvectorParallelPowerMethod.this.n;
				final int[] outdegree = DominantEigenvectorParallelPowerMethod.this.outdegree;
				final boolean markovian = DominantEigenvectorParallelPowerMethod.this.markovian;
				final double shift = DominantEigenvectorParallelPowerMethod.this.shift;
				final KahanSummation s = new KahanSummation(), rayleighQuotientNumerator = new KahanSummation(), rayleighQuotientDenominator = new KahanSummation();

				for(;;) {
					barrier.await();
					if (completed) return;
					final double[] oldRank = rank, newRank = previousRank;

					rayleighQuotientNumerator.reset();
					rayleighQuotientDenominator.reset();

					for(;;) {
						// Try to get another piece of work.
						final long start = nextNode.getAndAdd(GRANULARITY);
						if (start >= n) {
							nextNode.getAndAdd(-GRANULARITY);
							break;
						}

						final int end = (int)(Math.min(n, start + GRANULARITY));

						// for each node, enumerate predecessors and compute an updated value
						final NodeIterator nodeIterator = graph.nodeIterator((int)start);

						for(int i = (int)start; i < end; i++) {
							nodeIterator.nextInt();
							int indegree = nodeIterator.outdegree();
							s.reset();

							if (indegree != 0) {
								final int[] pred = nodeIterator.successorArray();
								if (markovian) while (indegree-- != 0) s.add(oldRank[pred[indegree]] / outdegree[pred[indegree]]);
								else while (indegree-- != 0) s.add(oldRank[pred[indegree]]);
							}

							final double old = oldRank[i];

							newRank[i] = s.value();
							if (shift != 0) newRank[i] -= shift * old;

							rayleighQuotientNumerator.add(newRank[i] * old);
							rayleighQuotientDenominator.add(old * old);
						}

						synchronized (progressLogger) {
							progressLogger.update(end - start);
						}
					}

					synchronized(DominantEigenvectorParallelPowerMethod.this) {
						DominantEigenvectorParallelPowerMethod.this.rayleighQuotientNumerator.add(rayleighQuotientNumerator.value());
						DominantEigenvectorParallelPowerMethod.this.rayleighQuotientDenominator.add(rayleighQuotientDenominator.value());
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

					// Rayleigh quotient
					lambda = rayleighQuotientNumerator.value() / rayleighQuotientDenominator.value();
					final double oneOverLambda = 1 / lambda;
					for(int i = rank.length; i-- != 0;) rank[i] *= oneOverLambda;
					lambda += shift;
					logger.info("Current estimate of the dominant eigenvalue: " + lambda);

					//System.err.println("lambda: " + lambda + " mu: " + norm.compute(rank) + " difference: " + (lambda - norm.compute(rank)));

					if (stoppingCriterion.shouldStop(DominantEigenvectorParallelPowerMethod.this)) completed = true;

					norm.normalize(rank, 1);
					rayleighQuotientNumerator.reset();
					rayleighQuotientDenominator.reset();

					// for(int i = n; i-- != 0;) rank[i] = (rank[i] + previousRank[i]) / 2;
					iterationLogger.setAndDisplay(iteration);
					if (completed) return;
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
		outdegree = null;
	}

	/**
	 * Returns a Properties object that contains all the parameters used by the computation.
	 *
	 * @param graphBasename the basename of the graph.
	 * @return a properties object that represent all the parameters used to calculate the rank.
	 */
	@Override
	public Properties buildProperties(String graphBasename) {
		final Properties prop = super.buildProperties(graphBasename);
		prop.setProperty("norm", norm);
		prop.setProperty("lambda", Double.toString(lambda - shift));
		prop.setProperty("markovian", Boolean.toString(markovian));
		prop.setProperty("shift", shift);
		return prop;
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException {

		SimpleJSAP jsap = new SimpleJSAP(DominantEigenvectorParallelPowerMethod.class.getName(), "Computes the dominant eigenvalue and eigenvector of a graph, given its transpose, using a parallel implementation of the power method."
				+ " The file <rankBasename>.properties stores metadata about the computation, whereas the file <rankBasename>.ranks stores the result as a sequence of doubles in DataInput format.",
			new Parameter[] {
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "Threshold to determine whether to stop."),
			new FlaggedOption("shift", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "shift", "A shift for the power method."),
			new Switch("markovian", 'M', "markovian", "Stocasticise the matrix."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph"),
			new FlaggedOption("norm", JSAP.STRING_PARSER, DEFAULT_DOMINANT_EIGENVECTOR_NORM.toString(), JSAP.NOT_REQUIRED, 'n', "norm", "Norm type. Possible values: " + Arrays.toString(Norm.values())),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("transposeBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped");
		final boolean markovian = jsapResult.getBoolean("markovian");
		final String graphBasename = jsapResult.getString("transposeBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final String norm = jsapResult.getString("norm");
		final double shift = jsapResult.getDouble("shift", 0);
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");

		ImmutableGraph graph = mapped? ImmutableGraph.loadMapped(graphBasename, progressLogger) : ImmutableGraph.load(graphBasename, progressLogger);

		DominantEigenvectorParallelPowerMethod pr = new DominantEigenvectorParallelPowerMethod(graph, threads, LOGGER);
		pr.markovian = markovian;
		pr.norm = Norm.valueOf(norm);
		pr.shift = shift;

		pr.stepUntil(or(new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(pr.rank, rankBasename +".ranks");
		pr.buildProperties(graphBasename).save(rankBasename + ".properties");
	}
}

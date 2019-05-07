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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.law.util.KahanSummation;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Properties;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

// RELEASE-STATUS: DIST

/** Computes a power series on a graph using a parallel implementation.
 *
 * <p>This class is a generic power series approximator. It computes iteratively finite truncations of power series of the form
 * <div style="text-align: center">
 * <var><b>v</b></var> <big>&Sigma;</big><sub><var>k</var> &ge; 0</sub> (&alpha;<var>M</var>)<sup><var>k</var></sup>.
 * </div>
 * where <var><b>v</b></var> is a <em>{@linkplain #preference preference vector}</em> that defaults to <b>1</b>, and
 * <var>M</var> is the graph adjacency matrix, possibly with stochasticised rows if {@link #markovian} is true.
 * Note that the {@link #step()} method is not available: due to the need for some synchronization logic, only {@link #stepUntil(StoppingCriterion)}
 * is available.
 *
 * <p><strong>Warning</strong>: Since we need to enumerate the <em>predecessors</em> a node,
 * you must pass to the {@linkplain #PowerSeries(ImmutableGraph, int, Logger) constructor} the <strong>transpose</strong>
 * of the graph.

 * <p>This class can be run using two different stopping criteria:
 * <ul>
 * <li>{@link #MAX_RATIO_STOPPING_CRITERION} stops when the maximum ratio between a component of the vector given by
 * the previous approximation multiplied by  <var>M</var> and the respective component in the previous approximation
 * is smaller than the reciprocal of {@linkplain #alpha &alpha;};
 * <li>{@link SpectralRanking.NormStoppingCriterion} stops when {@link #normDelta()},
 * which returns
 * the &#x2113;<sub>&#x221E;</sub> norm of the difference between the two last approximations,
 * is below a specified threshold.
 * </ul>
 *
 * <p>In the first case, this class computes <em>suitable vectors</em> that can be used to control
 * the error of Gau&szlig;&ndash;Seidel's method applied to {@linkplain KatzParallelGaussSeidel Katz's index} or {@linkplain PageRankParallelGaussSeidel PageRank}.
 * Details about the method are described by Sebastiano Vigna in &ldquo;<a href="http://vigna.di.unimi.it/papers.php#VigSNCSASOM">Supremum-Norm Convergence for Step-Asynchronous Successive Overrelaxation on M-matrices</a>&ldquo;, 2014.
 *
 * <p>In the second case, we compute Katz's index, or a pseudorank divided by 1 &minus; &alpha; (in both cases, the computation converges
 * more slowly than using {@link KatzParallelGaussSeidel} or even {@link PageRankParallelPowerSeries}, so
 * this feature is of marginal interest).
 *
 * <p>At the end of the computation, {@link  #scale} contains the scaling that has been applied to the {@linkplain #rank result}
 * so that it is normalized in &#x2113;<sub>&#x221E;</sub> norm. It is possible to obtain the unscaled result dividing all components of
 * the results by {@link #scale}. Note that when using the {@link #MAX_RATIO_STOPPING_CRITERION} if the parameter {@linkplain #alpha} is not smaller
 * than the reciprocal of the dominant eigenvalue the computation will stop with an error either because a lower bound proves this fact, or because
 * the scale will go below {@link #MIN_SCALE} (the latter event might also be due to a very bad non-normal transient behaviour, but
 * this shouldn't happen with real, non-pathological data).
 *
 * <p>During the computation, the maximum and minimum ratios between a component of the vector given by
 * the previous approximation multiplied by  <var>M</var> and the respective component in the previous approximation are printed;
 * they provide upper and lower bounds to the dominant eigenvalue by Collatz's theorem. At the end of the computation,
 * the current bounds can be found in {@link #maxRatio} and {@link #minRatio}.
 *
 * <p><strong>Warning</strong>: if the computation stops because of the {@link #MAX_RATIO_STOPPING_CRITERION},
 * the vector suitable for {@link #maxRatio} is stored in {@link #previousRank}, not in {@link #rank rank}, as the maximum ratio was evaluated
 * for {@link #previousRank} while {@link #rank rank} was computed. Moreover, if you provided a {@linkplain #preference preference vector}
 * with some zero component, you <strong>must</strong> check manually that the suitable vector obtained contains no zero entries.
 *
 * @see SpectralRanking
 *
 * @author Sebastiano Vigna
 */

public class PowerSeries extends SpectralRanking {
	private final static Logger LOGGER = LoggerFactory.getLogger(PowerSeries.class);

	/** Below this scale, we stop the iterative process. */
	public final static double MIN_SCALE = 1E-300;

	private final static class MaxRatioStoppingCriterion implements StoppingCriterion {
		public boolean shouldStop(final SpectralRanking spectralRanking) {
			if (! (spectralRanking instanceof PowerSeries)) throw new IllegalArgumentException(MaxRatioStoppingCriterion.class.getName() + " can be used with instances of " + PowerSeries.class.getName() + " only.");
			final PowerSeries powerSeries = (PowerSeries)spectralRanking;
			powerSeries.logger.info("Current max ratio: " + powerSeries.maxRatio + " (will stop below " + 1 / powerSeries.alpha + ")");
			return powerSeries.maxRatio * powerSeries.alpha < 1;
		}
	}

	/** A stopping criterion that stops when {@link PowerSeries#maxRatio} is smaller than the reciprocal of {@link PowerSeries#alpha}.
	 *
	 *  <p>Note that this criterion can be applied to instances of {@link PowerSeries} only.
	 */
	public static StoppingCriterion MAX_RATIO_STOPPING_CRITERION = new MaxRatioStoppingCriterion();

	/** If {@link #markovian} is true, the outdegrees. */
	private int[] outdegree;
	/** A progress logger monitoring each iteration. */
	private final ProgressLogger progressLogger;
	/** A progress logger monitoring the iterations. */
	private final ProgressLogger iterationLogger;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node to be picked. */
	private final AtomicLong nextNode;
	/** If true, the computation is over. */
	private volatile boolean completed;
	/** If true, the computation was interrupted by the detection of an error condition. */
	private volatile boolean interrupted;
	/** The barrier used to synchronize threads. */
	private volatile CyclicBarrier barrier;
	/** Keeps track of problems in  threads. */
	private volatile Throwable threadThrowable;
	/** The accumulator for the supremum norm of {@link #rank}. */
	private double norm;
	/** The accumulator for the supremum norm of {@link #rank} minus {@link #previousRank}. */
	private double normDelta;
	/** The maximum ratio between components. */
	public double maxRatio;
	/** The minimum ratio between components. */
	public double minRatio;
	/** The attenuation factor. Must be smaller than the reciprocal of the dominant eigenvalue. */
	public double alpha;
	/** If true, the matrix adjacency graph will be stochasticised, thus computing a pseudorank. */
	public boolean markovian;
	/** The overall scaling that has been applied to the current approximation. */
	public double scale;
	/** The preference vector to be used (or {@code null} if the uniform preference vector should be used). */
	public DoubleList preference;
	/** The approximation obtained after the last iteration (only meaningful after at least one step). */
	public double[] previousRank;

	/** Creates a new instance.
	 *
	 * @param transpose the tranpose of the graph on which the power series must be computed.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public PowerSeries(final ImmutableGraph transpose, final int requestedThreads, final Logger logger) {
		super(transpose, logger);
		progressLogger = new ProgressLogger(logger, "nodes");
		iterationLogger = new ProgressLogger(logger, "iterations");
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
		nextNode = new AtomicLong();
	}

	/** Creates a new instance.
	 *
	 * @param transpose the tranpose of the graph on which the power series must be computed.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 */
	public PowerSeries(final ImmutableGraph transpose, final int requestedThreads) {
			this(transpose, requestedThreads, LOGGER);
	}

	/** Creates a new instance.
	 *
	 * @param transpose the tranpose of the graph on which the power series must be computed.
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public PowerSeries(final ImmutableGraph transpose, final Logger logger) {
			this(transpose, 0, logger);
	}

	/** Creates a new instance.
	 *
	 * @param transpose the tranpose of the graph on which the power series must be computed.
	 */
	public PowerSeries(final ImmutableGraph transpose) {
		this(transpose, 0);
	}

	@Override
	public void init() throws IOException {
		super.init();
		if (alpha == 0) throw new IllegalArgumentException("The attenuation factor must be nonzero");
		logger.info("Attenuation factor: " + alpha);
		maxRatio = Double.NEGATIVE_INFINITY;
		minRatio = Double.POSITIVE_INFINITY;
		normDelta = norm = 0;
		interrupted = completed = false;
		// Creates the arrays, if necessary
		if (previousRank == null) previousRank = new double[n];

		// Check the preference vector
		if (preference != null) {
			if (preference.size() != n) throw new IllegalArgumentException("The preference vector size (" + preference.size() + ") is different from graph dimension (" + n + ").");
			logger.info("Using a specified preference vector");
			for(int i = n; i-- != 0;) rank[i] = preference.getDouble(i);
			scale = 1 / Norm.L_INFINITY.compute(rank);
			for(int i = n; i-- != 0;) rank[i] *= scale;
		}
		else {
			logger.info("Using the uniform preference vector");
			scale = 1;
			Arrays.fill(rank, 1);
		}

		if (markovian && outdegree == null) {
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

		logger.info("Completed.");
		iterationLogger.start();
	}

	private final class IterationThread extends Thread {
		private static final int GRANULARITY = 10000;

		public void run() {
			try {
				// We cache frequently used fields.
				final ImmutableGraph graph = PowerSeries.this.graph.copy();
				final int n = PowerSeries.this.n;
				final KahanSummation s = new KahanSummation();
				final boolean markovian = PowerSeries.this.markovian;
				final int[] outdegree = PowerSeries.this.outdegree;
				final double alpha = PowerSeries.this.alpha;
				final DoubleList preference = PowerSeries.this.preference;

				for(;;) {
					barrier.await();
					if (completed) return;
					final double[] oldRank = rank, newRank = previousRank;

					final double scale = PowerSeries.this.scale;
					double norm = 0, normDelta = 0, maxRatio = Double.NEGATIVE_INFINITY, minRatio = Double.POSITIVE_INFINITY;

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

							final double t =  alpha * s.value() + (preference != null ? scale * preference.getDouble(i) : scale);
							newRank[i] = t;
							norm = Math.max(norm, t);
							normDelta = Math.max(normDelta, Math.abs(t - oldRank[i]));
							if (oldRank[i] != 0) {
								final double ratio = s.value() / oldRank[i];
								maxRatio = Math.max(maxRatio, ratio);
								minRatio = Math.min(minRatio, ratio);
							}
						}

						synchronized (progressLogger) {
							progressLogger.update(end - start);
						}

					}

					synchronized(PowerSeries.this) {
						PowerSeries.this.normDelta = Math.max(PowerSeries.this.normDelta, normDelta);
						PowerSeries.this.norm = Math.max(PowerSeries.this.norm, norm);
						PowerSeries.this.maxRatio = Math.max(PowerSeries.this.maxRatio, maxRatio);
						PowerSeries.this.minRatio = Math.min(PowerSeries.this.minRatio, minRatio);
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

					final double s = 1 / norm;
					for(int i = n; i-- != 0;) {
						// We must keep rank and previousRank scaled in the same way.
						rank[i] *= s;
						previousRank[i] *= s;
					}
					scale *= s;

					logger.info("Scale: " + scale + "; min ratio: " + minRatio + "; max ratio: " + maxRatio);

					if (maxRatio == Double.NEGATIVE_INFINITY) {
						logger.error("The preference vector is null");
						interrupted = completed = true;
						return;
					}

					if (alpha * minRatio >= 1) {
						logger.error("The current lower bound for the spectral radius (" + minRatio + ") is larger than or equal to the inverse of the attenuation factor (" + 1 / alpha + ")");
						interrupted = completed = true;
						return;
					}

					if (scale < MIN_SCALE) {
						logger.error("Scale went below " + MIN_SCALE + ": " + 1 / alpha + " is likely to be larger than or equal to the spectral radius (current estimate: [" + minRatio + ".." + maxRatio + "])");
						interrupted = completed = true;
						return;
					}

					iterationLogger.setAndDisplay(iteration);

					if (stoppingCriterion.shouldStop(PowerSeries.this)) {
						completed = true;
						return;
					}
				}

				maxRatio = normDelta = norm = 0;
				minRatio = Double.POSITIVE_INFINITY;
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
		return normDelta;
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
	 * @param graphBasename file name of the graph
	 * @param preferenceFilename file name of preference vector. It can be {@code null}.
	 * @return a properties object that represent all the parameters used to calculate the rank.
	 */
	public Properties buildProperties(String graphBasename, String preferenceFilename) {
		final Properties prop = super.buildProperties(graphBasename);
		prop.setProperty("alpha", Double.toString(alpha));
		prop.setProperty("maxratio", maxRatio);
		prop.setProperty("minratio", minRatio);
		prop.setProperty("normdelta", normDelta);
		prop.setProperty("markovian", markovian);
		prop.setProperty("scale", Double.toString(scale));
		prop.setProperty("preferencefilename", preferenceFilename);
		return prop;
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException, ClassNotFoundException {

		SimpleJSAP jsap = new SimpleJSAP(PowerSeries.class.getName(), "Computes a power series on a graph, given its transpose, using a parallel implementation."
				+ " Alternatively, computes a suitable vector for the graph adjacency matrix, possibly stochasticised."
				+ " The file <rankBasename>.properties stores metadata about the computation, whereas the file <rankBasename>.ranks stores the result as a sequence of doubles in DataInput format.",
			new Parameter[] {
			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'a', "alpha", "Attenuation factor (must be smaller than the dominant eigenvalue)."),
			new Switch("markovian", 'M', "markovian", "Stochasticise the matrix."),
			new Switch("suitable", 'S', "suitable", "Compute a vector suitable for attenuation factors smaller than alpha."),
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "Threshold to determine whether to stop (not used for suitable vectors)."),
			new FlaggedOption("preferenceVector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "preference-vector", "A preference vector stored as a vector of binary doubles."),
			new FlaggedOption("preferenceObject", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'P', "preference-object", "A preference vector stored as a serialised DoubleList."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("transposeBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final double alpha = jsapResult.getDouble("alpha");
		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final boolean suitable = jsapResult.getBoolean("suitable");
		final String transposeBasename = jsapResult.getString("transposeBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final int threads = jsapResult.getInt("threads");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");

		final ImmutableGraph transpose = mapped? ImmutableGraph.loadMapped(transposeBasename, progressLogger) : ImmutableGraph.load(transposeBasename, progressLogger);

		DoubleList preference = null;
		String preferenceFilename = null;
		if (jsapResult.userSpecified("preferenceVector"))
			preference = DoubleArrayList.wrap(BinIO.loadDoubles(preferenceFilename = jsapResult.getString("preferenceVector")));

		if (jsapResult.userSpecified("preferenceObject")) {
			if (jsapResult.userSpecified("preferenceVector")) throw new IllegalArgumentException("You cannot specify twice the preference vector");
			preference = (DoubleList)BinIO.loadObject(preferenceFilename = jsapResult.getString("preferenceObject"));
		}

		PowerSeries pr = new PowerSeries(transpose, threads, LOGGER);
		pr.alpha = alpha;
		pr.markovian = jsapResult.getBoolean("markovian");
		pr.preference = preference;

		pr.stepUntil(or(suitable ? MAX_RATIO_STOPPING_CRITERION : new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(suitable ? pr.previousRank : pr.rank, rankBasename + ".ranks");
		pr.buildProperties(transposeBasename, preferenceFilename).save(rankBasename + ".properties");
	}
}

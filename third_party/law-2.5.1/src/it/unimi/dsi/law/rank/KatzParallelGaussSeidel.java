package it.unimi.dsi.law.rank;

import java.io.DataInput;
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
import it.unimi.dsi.fastutil.doubles.DoubleIterators;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.law.util.KahanSummation;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Properties;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

// RELEASE-STATUS: DIST

/** Computes Katz's index using a parallel implementation of the Gau&szlig;&ndash;Seidel method; this is the implementation of choice to be used when computing Katz's index.
 * It uses less memory (just one vector of doubles) and, experimentally, converges faster than any other implementation. Moreover, it
 * scales linearly with the number of cores.
 *
 * <p><strong>Warning</strong>: Since we need to enumerate the <em>predecessors</em> a node,
 * you must pass to the {@linkplain #KatzParallelGaussSeidel(ImmutableGraph, int, Logger) constructor} the <strong>transpose</strong>
 * of the graph.
 *
 * <p>This class approximates the infinite summation
 * <div style="text-align: center">
 * <var><b>v</b></var> <big>&Sigma;</big><sub><var>k</var> &ge; 0</sub> (&alpha;<var>G</var>)<sup><var>k</var></sup> = <var><b>v</b></var>(1 &minus; &alpha;<var>G</var>)<sup>-1</sup>
 * </div>
 * by solving iteratively the linear system
 * <div style="text-align: center">
 * <var><b>x</b></var> (1 &minus; &alpha;<var>G</var>) = <var><b>v</b></var>,
 * </div>
 * where <var><b>v</b></var> is a <em>{@linkplain #preference preference vector}</em> that defaults to <b>1</b>, and
 * <var>G</var> is the graph adjacency matrix. Note that the {@link #step()} method is not available: due to the need for some synchronization logic, only {@link #stepUntil(StoppingCriterion)}
 * is available.
 *
 * <p>Technically, the iteration performed by this class is a <em>step-asynchronous</em> Gau&szlig;&ndash;Seidel iteration: we simply start a number of
 * threads, and each thread updates a value using a Gau&szlig;&ndash;Seidel-like rule.
 * As a result, each update uses some old and some new values: in other
 * words, the <em>regular splitting</em>
 * <div style="text-align: center; margin: 1em">
 *    <var>M &minus; N</var> = 1 &minus; &alpha;<var>G</var>
 * </div>
 * of the matrix associated to each update is always different (in a Gau&szlig;&ndash;Seidel iteration,
 * <var>M</var> is upper triangular, and <var>N</var> is strictly lower triangular). Nonetheless, it is easy to check that
 * <var>M</var> is still (up to permutation) upper triangular and invertible, independently of the specific update sequence.
 *
 * <p>{@link #normDelta()} returns the following values:
 * <ul>
 * <li>if a {@linkplain #normVector(double[], double) suitable norm vector has been set}, an upper bound on the error (the &#x2113;<sub>&#x221E;</sub> distance from the rank to be computed);
 * <li>otherwise, an estimate of the &#x2113;<sub>&#x221E;</sub> norm of the residual obtained by multiplying the &#x2113;<sub>&#x221E;</sub> norm
 * of the difference between the last two approximations by the &#x2113;<sub>&#x221E;</sub> norm of &alpha;<var>G</var>
 * (i.e., the maximum indegree multiplied by &alpha;).</ul>
 *
 * <p>To be able to set a norm vector, you need to use {@link PowerSeries} to compute a suitable vector.
 * To do so, you must provide an &alpha; and use the {@link PowerSeries#MAX_RATIO_STOPPING_CRITERION}. If the computation
 * terminates without errors with maximum ratio &sigma;, the {@linkplain PowerSeries#previousRank resulting vector} can be used
 * with this class for all &alpha; &lt; 1 / &sigma; (strictness
 * is essential). Note that this is the <em>only</em> way to obtain a bound on the error (unless
 * your graph is so small that you can evaluate the &#x2113;<sub>&#x221E;</sub> norm of (1 &minus; &alpha;<var>G</var>)<sup>-1</sup> and use it to bound the error using the residual).
 * Details about the method are described by Sebastiano Vigna in &ldquo;<a href="http://vigna.di.unimi.it/papers.php#VigSNCSASOM">Supremum-Norm Convergence for Step-Asynchronous Successive Overrelaxation on M-matrices</a>&ldquo;, 2014.
 *
 * @see PageRank
 * @see SpectralRanking
 * @see PowerSeries
 *
 * @author Sebastiano Vigna
 */

public class KatzParallelGaussSeidel extends SpectralRanking {
	private final static Logger LOGGER = LoggerFactory.getLogger(KatzParallelGaussSeidel.class);

	/** A progress logger monitoring each iteration. */
	private final ProgressLogger progressLogger;
	/** A progress logger monitoring the iterations. */
	private final ProgressLogger iterationLogger;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node to be picked. */
	private final AtomicLong nextNode;
	/** The norm of the difference vector between the new approximation and the previous one. */
	private double normDelta;
	/** If true, the computation is over. */
	private volatile boolean completed;
	/** The barrier used to synchronize threads. */
	private volatile CyclicBarrier barrier;
	/** Keeps track of problems in threads. */
	private volatile Throwable threadThrowable;
	/** An array of bytes containing the opposite of a lower bound on the binary logarithm of the elements of a norm vector, or {@code null} to stop the computation using residue estimation. */
	private byte[] normVector;
	/** The value for which {@link #normVector} is suitable. */
	private double sigma;
	/** The maximum indegree, or -1 if the maximum indegree has not been computed yet. */
	private int maxIndegree = -1;

	/** The attenuation factor. Must be smaller than the reciprocal of the dominant eigenvalue. */
	public double alpha;
	/** The preference vector to be used (or {@code null} if the uniform preference vector should be used). */
	public DoubleList preference;

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute Katz's index.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public KatzParallelGaussSeidel(final ImmutableGraph transpose, final int requestedThreads, final Logger logger) {
		super(transpose, logger);
		progressLogger = new ProgressLogger(logger, "nodes");
		iterationLogger = new ProgressLogger(logger, "iterations");
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
		nextNode = new AtomicLong();
	}

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute Katz's index.
	 */
	public KatzParallelGaussSeidel(final ImmutableGraph transpose) {
		this(transpose, 0, LOGGER);
	}

	/** Sets the norm vector.
	 *
	 * @param normVectorFilename a file containing a norm vector as a list of doubles in {@link DataInput} format, or {@code null} for no norm vector.
	 * @param sigma the value for which the provided norm vector is suitable.
	 */
	public void normVector(final String normVectorFilename, final double sigma) throws IOException {
		normVector = normVectorFilename == null ? null : approximateNormVector(BinIO.asDoubleIterator(normVectorFilename));
		this.sigma = sigma;
	}

	/** Sets the norm vector.
	 *
	 * @param normVector the new norm vector.
	 * @param sigma the value for which the provided norm vector is suitable.
	 */
	public void normVector(final double[] normVector, final double sigma) {
		this.normVector = approximateNormVector(DoubleIterators.wrap(normVector));
		this.sigma = sigma;
	}

	@Override
	public void init() throws IOException {
		super.init();

		logger.info("Attentuation factor: " + alpha);

		if (normVector != null && alpha >= 1 / sigma) throw new IllegalStateException("The specified norm vector can be used only with values of alpha smaller than " + 1 / sigma);

		// Check the preference vector
		if (preference != null) {
			if (preference.size() != n) throw new IllegalArgumentException("The preference vector size (" + preference.size() + ") is different from graph dimension (" + n + ").");
			logger.info("Using a specified preference vector");
			for(int i = n; i-- != 0;) rank[i] = preference.getDouble(i);
		}
		else {
			logger.info("Using the uniform preference vector");
			Arrays.fill(rank, 1);
		}

		if (normVector == null && maxIndegree == -1) {
			// TODO: refactor using outdegrees().
			final NodeIterator nodeIterator = graph.nodeIterator();
			for(int i = n; i-- != 0;) {
				nodeIterator.nextInt();
				maxIndegree = Math.max(maxIndegree, nodeIterator.outdegree());
			}
		}

		// Replace initialization
		progressLogger.expectedUpdates = n;
		progressLogger.start("Computing initial dangling rank...");

		progressLogger.done();

		completed = false;
		logger.info("Completed.");
		iterationLogger.start();
	}

	private final class IterationThread extends Thread {
		private static final int GRANULARITY = 10000;

		public void run() {
			try {
				// We cache frequently used fields.
				final ImmutableGraph graph = KatzParallelGaussSeidel.this.graph.copy();
				final int n = KatzParallelGaussSeidel.this.n;
				final double alpha = KatzParallelGaussSeidel.this.alpha;
				final double rank[] = KatzParallelGaussSeidel.this.rank;
				final byte[] normVector = KatzParallelGaussSeidel.this.normVector;
				final DoubleList preference = KatzParallelGaussSeidel.this.preference;
				final KahanSummation s = new KahanSummation();

				for(;;) {
					barrier.await();
					if (completed) return;

					for(;;) {
						// Try to get another piece of work.
						final long start = nextNode.getAndAdd(GRANULARITY);
						if (start >= n) {
							nextNode.getAndAdd(-GRANULARITY);
							break;
						}

						final int end = (int)(Math.min(n, start + GRANULARITY));

						// for each node, enumerate predecessors and compute an updated value
						double normDelta = 0;
						final NodeIterator nodeIterator = graph.nodeIterator((int)start);

						for(int i = (int)start; i < end; i++) {
							nodeIterator.nextInt();

							double sigma = 0;
							double selfLoopFactor = 1;
							s.reset();
							int indegree = nodeIterator.outdegree();

							if (indegree != 0) {
								final int[] pred = nodeIterator.successorArray();
								//Determine the rank from all incoming real links except possibly for a self-loop.
								while(indegree-- != 0) {
									final int currPred = pred[indegree];
									if (i == currPred) selfLoopFactor = 1 - alpha;
									else sigma += rank[currPred];
								}
							}

							sigma = ((preference != null ? preference.getDouble(i) : 1) + alpha * sigma) / selfLoopFactor;

							// update the supremum of vector difference between the new and old rank

							if (normVector != null) normDelta = Math.max(normDelta, Math.abs(sigma - rank[i]) * (1L << (0xFF & normVector[i])));
							else normDelta = Math.max(normDelta, Math.abs(sigma - rank[i]));

							//update the rank
							rank[i] = sigma;
						}

						synchronized (progressLogger) {
							progressLogger.update(end - start);
						}

						synchronized (KatzParallelGaussSeidel.this) {
							KatzParallelGaussSeidel.this.normDelta = Math.max(KatzParallelGaussSeidel.this.normDelta, normDelta);
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

					iterationLogger.setAndDisplay(iteration);

					if (stoppingCriterion.shouldStop(KatzParallelGaussSeidel.this)) {
						completed = true;
						return;
					}
				}

				normDelta = 0;
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
		return normVector == null ? alpha * normDelta * maxIndegree : (alpha * sigma) * normDelta / (1 - alpha * sigma);
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
		prop.setProperty("norm", normDelta);
		prop.setProperty("preferencefilename", preferenceFilename);
		return prop;
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException, ClassNotFoundException {

		SimpleJSAP jsap = new SimpleJSAP(KatzParallelGaussSeidel.class.getName(), "Computes Katz's index of a graph, given its transpose, using a parallel implementation of Gauss-Seidel's method."
				+ " The file <rankBasename>.properties stores metadata about the computation, whereas the file <rankBasename>.ranks stores the result as a sequence of doubles in DataInput format.",
			new Parameter[] {
			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'a', "alpha", "Attenuation factor (must be smaller than the reciprocal of the dominant eigenvalue)."),
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "Threshold to determine whether to stop."),
			new FlaggedOption("preferenceVector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "preference-vector", "A preference vector stored as a vector of binary doubles."),
			new FlaggedOption("preferenceObject", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'P', "preference-object", "A preference vector stored as a serialised DoubleList."),
			new FlaggedOption("normVector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n', "norm-vector", "A vector inducing the correct weighted supremum norm."),
			new FlaggedOption("sigma", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "sigma", "The value for which the norm vector is suitable (i.e., the maximum ratio from its properties)."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("transposeBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final String graphBasename = jsapResult.getString("transposeBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final String normVectorFilename = jsapResult.getString("normVector");
		if (normVectorFilename != null && ! jsapResult.userSpecified("sigma")) throw new IllegalArgumentException("You must specify the sigma for which the norm vector is suitable");
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

		final KatzParallelGaussSeidel pr = new KatzParallelGaussSeidel(graph, threads, LOGGER);
		pr.alpha = jsapResult.getDouble("alpha");
		pr.preference = preference;
		if (normVectorFilename != null) pr.normVector(normVectorFilename, jsapResult.getDouble("sigma"));

		pr.stepUntil(or(new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(pr.rank, rankBasename + ".ranks");
		pr.buildProperties(graphBasename, preferenceFilename).save(rankBasename + ".properties");

	}
}

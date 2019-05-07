package it.unimi.dsi.law.rank;

import java.io.DataInput;
import java.io.IOException;
import java.util.BitSet;
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
import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

// RELEASE-STATUS: DIST

/** Computes PageRank using a parallel (multicore) implementation of the {@linkplain PageRankGaussSeidel Gau&szlig;&ndash;Seidel} method.
 *
 * <p><strong>Note</strong>: this is the implementation of choice to be used when computing PageRank. It uses less memory (one vector
 * of doubles plus one vector of integers) and, experimentally, converges faster than any other implementation. Moreover, it
 * scales linearly with the number of cores.
 *
 * <p><strong>Warning</strong>: Since we need to enumerate the <em>predecessors</em> a node,
 * you must pass to the {@linkplain #PageRankParallelGaussSeidel(ImmutableGraph, int, Logger) constructor} the <strong>transpose</strong>
 * of the graph.
 *
 * <p>Technically, the iteration performed by this class is <em>not</em> a Gau&szlig;&ndash;Seidel iteration: we simply start a number of
 * threads, and each thread updates a value using a Gau&szlig;&ndash;Seidel-like rule.
 * As a result, each update uses some old and some new values: in other
 * words, the <em>regular splitting</em>
 * <div style="text-align: center; margin: 1em">
 *    <var>M &minus; N</var> = <var>I</var> &minus; &alpha; (<var>P</var> + <var><b>u</b></var><sup><i>T</i></sup><var><b>d</b></var>)
 * </div>
 * of the matrix associated to each update is always different (in a Gau&szlig;&ndash;Seidel iteration,
 * <var>M</var> is upper triangular, and <var>N</var> is strictly lower triangular). Nonetheless, it is easy to check that
 * <var>M</var> is still (up to permutation) upper triangular and invertible, independently of the specific update sequence.
 *
 * <p>Note that the {@link #step()} method is not available: due to the need for some synchronization logic, only {@link #stepUntil(StoppingCriterion)}
 * is available.
 *
 * <p>The {@link #normDelta()} method returns the following values:
 * <ul>
 * <li>if a {@linkplain #normVector(double[], double) suitable norm vector has been set}, an upper bound on the error (the &#x2113;<sub>&#x221E;</sub> distance from the rank to be computed);
 * <li>otherwise, an upper bound to the &#x2113;<sub>1</sub> norm of the error, obtained multiplying by
 * &alpha; / (1 &minus; &alpha;) the &#x2113;<sub>1</sub> norm of the difference between the last two approximations (this idea arose in discussions with David Gleich).
 * </ul>
 *
 * <p>To be able to set a norm vector, you need to set the {@link #pseudoRank} flag and use {@link PowerSeries}
 * (setting the {@linkplain PowerSeries#markovian Markovian} flag) to compute a suitable vector.
 * To do so, you must provide an &alpha; and use the {@link PowerSeries#MAX_RATIO_STOPPING_CRITERION}. If the computation
 * terminates without errors with maximum ratio &sigma;, the {@linkplain PowerSeries#previousRank resulting vector} can be used
 * with this class to compute pseudoranks for all &alpha; &lt; 1 / &sigma; (strictness
 * is essential). Note that the &#x2113;<sub>1</sub> norm of the error bounds the &#x2113;<sub>&#x221E;</sub>.
 *
 * <p>With respect to the description of the exact algorithm in {@link PageRankGaussSeidel}, we operate a simplification that is
 * essentially in obtaining a coherent update without incurring in too much synchronization: the rank associated with
 * dangling nodes is computed at the end of each computation, and used unchanged throughout the whole iteration. This corresponds to
 * permuting the array so that dangling nodes come out last.
 *
 * @see PageRankGaussSeidel
 * @see PageRank
 * @see SpectralRanking
 *
 * @author Sebastiano Vigna
 */

public class PageRankParallelGaussSeidel extends PageRank {
	private final static Logger LOGGER = LoggerFactory.getLogger(PageRankParallelGaussSeidel.class);

	/** A progress logger monitoring each iteration. */
	private final ProgressLogger progressLogger;
	/** A progress logger monitoring the iterations. */
	private final ProgressLogger iterationLogger;
	/** The number of threads. */
	private final int numberOfThreads;
	/** The next node to be picked. */
	private final AtomicLong nextNode;
	/** The rank lost through dangling nodes, accumulated incrementally. */
	private double danglingRankAccumulator;
	/** The amount of ranking in dangling nodes computed at the previous iteration. */
	private double danglingRank;
	/** The &#x2113;<sub>1</sub> norm of the difference between the new approximation and the previous one,
	 * if {@link #normVector} is {@code null}; the {@link #normVector}-weighted supremum norm of the same vector, otherwise. */
	private double normDelta;
	/** The outdegree of each node (initialized after the first computation). */
	public int[] outdegree;
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
	/** If true, an everywhere zero dangling-node distribution will be simulated, resulting in the computation of a pseudorank. */
	public boolean pseudoRank;


	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute PageRank.
	 * @param requestedThreads the requested number of threads (0 for {@link Runtime#availableProcessors()}).
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public PageRankParallelGaussSeidel(final ImmutableGraph transpose, final int requestedThreads, final Logger logger) {
		super(transpose, logger);
		progressLogger = new ProgressLogger(logger, "nodes");
		iterationLogger = new ProgressLogger(logger, "iterations");
		numberOfThreads = requestedThreads != 0 ? requestedThreads : Runtime.getRuntime().availableProcessors();
		nextNode = new AtomicLong();
	}

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute PageRank.
	 */
	public PageRankParallelGaussSeidel(final ImmutableGraph transpose) {
		this(transpose, 0, LOGGER);
	}

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute PageRank.
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public PageRankParallelGaussSeidel(final ImmutableGraph transpose, final Logger logger) {
		this(transpose, 0, logger);
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

		if (normVector != null) {
			if (! pseudoRank) throw new IllegalStateException("Norm vectors can be used only when computing pseudoranks");
			if (alpha >= 1 / sigma) throw new IllegalStateException("The specified norm vector can be used only with values of alpha smaller than " + 1 / sigma);
		}

		if (outdegree == null) {
			// We allocate and compute the outdegree vector.
			outdegree = new int[n];
			// TODO: refactor using .outdegrees()
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

		progressLogger.expectedUpdates = n;
		progressLogger.start("Computing initial dangling rank...");

		danglingRank = 0;
		/* The number of dangling nodes. */
		int danglingNodes = 0;
		for (int i = n; i-- != 0;) {
			if (outdegree[i] == 0 || buckets != null && buckets.get(i)) {
				danglingRank += rank[i];
				if (outdegree[i] == 0) danglingNodes++;
			}
		}

		progressLogger.done();
		logger.info(danglingNodes + " dangling nodes");
		if (buckets != null) logger.info(buckets.cardinality() + " buckets");
		logger.info("Initial dangling rank: " + danglingRank);

		normDelta = danglingRankAccumulator = 0;
		completed = false;
		logger.info("Completed.");
		iterationLogger.start();
	}

	private final class IterationThread extends Thread {
		private static final int GRANULARITY = 10000;

		public void run() {
			try {
				// We cache frequently used fields.
				final ImmutableGraph graph = PageRankParallelGaussSeidel.this.graph.copy();
				final int n = PageRankParallelGaussSeidel.this.n;
				final double oneMinusAlpha = 1 - alpha;
				final double oneMinusAlphaOverN = oneMinusAlpha / n;
				final double[] rank = PageRankParallelGaussSeidel.this.rank;
				final int[] outdegree = PageRankParallelGaussSeidel.this.outdegree;
				final BitSet buckets = PageRankParallelGaussSeidel.this.buckets;
				final boolean pseudoRank = PageRankParallelGaussSeidel.this.pseudoRank;
				final double alpha = PageRankParallelGaussSeidel.this.alpha;
				final DoubleList danglingNodeDistribution = PageRankParallelGaussSeidel.this.danglingNodeDistribution;
				final DoubleList preference = PageRankParallelGaussSeidel.this.preference;
				final KahanSummation s = new KahanSummation();

				for(;;) {
					barrier.await();
					if (completed) return;
					final double danglingRank = PageRankParallelGaussSeidel.this.danglingRank;

					for(;;) {
						// Try to get another piece of work.
						final long start = nextNode.getAndAdd(GRANULARITY);
						if (start >= n) {
							nextNode.getAndAdd(-GRANULARITY);
							break;
						}

						final int end = (int)(Math.min(n, start + GRANULARITY));

						// for each node, enumerate predecessors and compute an updated value
						double danglingRankAccumulator = 0, norm = 0;
						final NodeIterator nodeIterator = graph.nodeIterator((int)start);

						for(int i = (int)start; i < end; i++) {
							nodeIterator.nextInt();
							s.reset();
							boolean hasLoop = false;

							//Determine the rank from all incoming real links except possibly self link.
							final int[] pred = nodeIterator.successorArray();
							for(int indegree = nodeIterator.outdegree(); indegree-- != 0;) {
								final int currPred = pred[indegree];
								// Skip buckets
								if (buckets != null && buckets.get(pred[indegree])) continue;
								if (i == currPred) hasLoop = true;
								else s.add(rank[currPred] / outdegree[currPred]);
							}

							double selfDanglingRank, selfLoopFactor;
							//Determine the diagonal rank contribution
							if (outdegree[i] == 0 || buckets != null && buckets.get(i)) { //i is a dangling node
								selfDanglingRank = rank[i];
								selfLoopFactor = pseudoRank ? 1 :
									(danglingNodeDistribution != null) ? 1 - alpha * danglingNodeDistribution.getDouble(i)
									: 1.0 - alpha / n;
							} else {
								selfDanglingRank = 0;
								selfLoopFactor = hasLoop ? 1 - alpha / outdegree[i] : 1; //i has no selfloop and it is not dangling
							}

							if (! pseudoRank) s.add(danglingNodeDistribution != null ? (danglingRank - selfDanglingRank) * danglingNodeDistribution.getDouble(i) : (danglingRank - selfDanglingRank) / n);

							final double newRank = ((preference != null ? oneMinusAlpha * preference.getDouble(i) : oneMinusAlphaOverN) + alpha * s.value()) / selfLoopFactor;

							if (outdegree[i] == 0 || buckets != null && buckets.get(i)) danglingRankAccumulator += newRank;

							if (normVector != null) norm = Math.max(norm, Math.abs(newRank - rank[i]) * (1L << (0xFF & normVector[i])));
							else norm += Math.abs(newRank - rank[i]);

							//update the rank
							rank[i] = newRank;
						}

						synchronized (progressLogger) {
							progressLogger.update(end - start);
						}

						synchronized (PageRankParallelGaussSeidel.this) {
							PageRankParallelGaussSeidel.this.danglingRankAccumulator += danglingRankAccumulator;
							if (normVector != null) PageRankParallelGaussSeidel.this.normDelta = Math.max(PageRankParallelGaussSeidel.this.normDelta, norm);
							else PageRankParallelGaussSeidel.this.normDelta += norm;
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

					/*
					// Compute the supremum norm of the residual
					double res = 0;
					double res1 = 0;
					double err = 0;
					final NodeIterator nodeIterator = graph.nodeIterator();
					for(int i = 0; i < n ; i++) {
						nodeIterator.nextInt();
						double prod = 0;
						final LazyIntIterator successors = nodeIterator.successors();
						for(int s; (s = successors.nextInt()) != -1;) prod += rank[s] / outdegree[s];
						final double pref = preference == null ? 1. / n : preference.getDouble(i);
						final double delta = Math.abs(rank[i]
								- alpha * prod
								- alpha * danglingRankAccumulator * pref
								- (1 - alpha) * pref);
						if (res < delta) res = delta;
						res1 += delta;
					}

					LOGGER.info("Supremum norm of the residual: " + res);
					LOGGER.info("l_1 norm of the residual: " + res1);
					LOGGER.info("Bound on the l_1 norm of the error: " + normDelta() / (1 - alpha));
					LOGGER.info("Bound on the supremum norm of the error: " + (1 + alpha) * res / (1 - alpha));
					LOGGER.info("Supremum norm of the error: " + err);
					if (err > (1 + alpha) * res / (1 - alpha)) LOGGER.warn("Wrong bound on error");
					if (res1 > normDelta()) LOGGER.warn("Wrong bound on residual: " + res1 + " > " + normDelta());
					*/

					if (stoppingCriterion.shouldStop(PageRankParallelGaussSeidel.this)) {
						completed = true;
						return;
					}

					danglingRank = danglingRankAccumulator;
					danglingRankAccumulator = 0;
				}

				normDelta = danglingRankAccumulator = 0;
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

	/** Return the following values: if a {@linkplain #normVector(double[], double) suitable norm vector has been set}, an upper bound on the error (the &#x2113;<sub>&#x221E;</sub> distance from the rank to be computed);
	 * otherwise, an upper bound to the &#x2113;<sub>1</sub> norm of the error, obtained multiplying by
	 * &alpha; / (1 &minus; &alpha;) the &#x2113;<sub>1</sub> norm of the difference between the last two approximations (this idea arose in discussions with David Gleich).
	 *
	 * @return an upper bound on the error.
	 */
	@Override
	public double normDelta() {
		return normVector == null ? normDelta * alpha / (1 - alpha) :  (alpha * sigma) * normDelta / (1 - alpha * sigma);
	}

	@Override
	public void clear() {
		super.clear();
		outdegree = null;
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException, ClassNotFoundException {

		final SimpleJSAP jsap = new SimpleJSAP(PageRankParallelGaussSeidel.class.getName(), "Computes PageRank of a graph, given its transpose, using a parallel implementation of Gauss-Seidel's method."
				+ " The file <rankBasename>.properties stores metadata about the computation, whereas the file <rankBasename>.ranks stores the result as a sequence of doubles in DataInput format.",
			new Parameter[] {
			new Switch("expand", 'e', "expand", "Expand the graph to increase speed (no compression)."),
			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, Double.toString(PageRank.DEFAULT_ALPHA), JSAP.NOT_REQUIRED, 'a', "alpha", "Damping factor."),
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "Threshold (in l_1 norm, if no norm vector has been specified; in the weighted supremum norm otherwise) to determine whether to stop."),
			new FlaggedOption("preferenceVector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "preference-vector", "A preference vector stored as a vector of binary doubles."),
			new FlaggedOption("preferenceObject", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'P', "preference-object", "A preference vector stored as a serialised DoubleList."),
			new Switch("pseudoRank", JSAP.NO_SHORTFLAG, "pseudorank", "Compute pseudoranks (the dangling preference is set to 0)."),
			new FlaggedOption("normVector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n', "norm-vector", "A vector inducing the correct weighted supremum norm."),
			new FlaggedOption("sigma", JSAP.DOUBLE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "sigma", "The value for which the norm vector is suitable (i.e., the maximum ratio from its properties)."),
			new FlaggedOption("buckets", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "buckets", "The buckets of the graph; if supplied, buckets will be treated as dangling nodes."),
			new Switch("mapped", 'm', "mapped", "Use loadMapped() to load the graph."),
			new Switch("strongly", 'S', "strongly", "use the preference vector to redistribute the dangling rank."),
			new FlaggedOption("threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically."),
			new UnflaggedOption("transposeBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean mapped = jsapResult.getBoolean("mapped", false);
		final boolean strongly = jsapResult.getBoolean("strongly", false);
		final String graphBasename = jsapResult.getString("transposeBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final String normVectorFilename = jsapResult.getString("normVector");
		if (normVectorFilename != null && ! jsapResult.userSpecified("sigma")) throw new IllegalArgumentException("You must specify the sigma for which the norm vector is suitable");
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

		PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(graph, threads, LOGGER);
		pr.alpha = jsapResult.getDouble("alpha");
		pr.preference = preference;
		pr.buckets = (BitSet)(buckets == null ? null : BinIO.loadObject(buckets));
		pr.stronglyPreferential = strongly;
		pr.pseudoRank = jsapResult.userSpecified("pseudoRank");
		if (normVectorFilename != null) pr.normVector(normVectorFilename, jsapResult.getDouble("sigma"));

		// cycle until we reach maxIter iterations or the norm is less than the given threshold (whichever comes first)
		pr.stepUntil(or(new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(pr.rank, rankBasename + ".ranks");
		pr.buildProperties(graphBasename, preferenceFilename, null).save(rankBasename + ".properties");
	}
}

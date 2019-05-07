package it.unimi.dsi.law.big.rank;

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


import java.io.DataInput;
import java.io.IOException;
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

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.bytes.ByteBigArrays;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.law.util.KahanSummation;
import it.unimi.dsi.logging.ProgressLogger;

// RELEASE-STATUS: DIST

/** A big version of {@link it.unimi.dsi.law.rank.PageRankParallelGaussSeidel}.
 * @see it.unimi.dsi.law.rank.PageRankParallelGaussSeidel
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
	public long[][] outdegree;
	/** If true, the computation is over. */
	private volatile boolean completed;
	/** The barrier used to synchronize threads. */
	private volatile CyclicBarrier barrier;
	/** Keeps track of problems in threads. */
	private volatile Throwable threadThrowable;
	/** An big array of bytes containing the opposite of a lower bound on the binary logarithm of the elements of a norm vector, or {@code null} to stop the computation using residue estimation. */
	private byte[][] normVector;
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
	public void normVector(final double[][] normVector, final double sigma) {
		final DoubleIterator it = (new DoubleBigArrayBigList(normVector)).listIterator();
		this.normVector = approximateNormVector(it);
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
			outdegree = LongBigArrays.newBigArray(n);
			// TODO: refactor using .outdegrees()
			progressLogger.expectedUpdates = n;
			progressLogger.start("Computing outdegrees...");

			final NodeIterator nodeIterator = graph.nodeIterator();
			for (long i = n; i-- != 0;) {
				nodeIterator.nextLong();
				final long[][] pred = nodeIterator.successorBigArray();
				for (long d = nodeIterator.outdegree(); d-- != 0;) {
					LongBigArrays.incr(outdegree, LongBigArrays.get(pred, d));
				}
				progressLogger.lightUpdate();
			}

			progressLogger.done();
		}

		progressLogger.expectedUpdates = n;
		progressLogger.start("Computing initial dangling rank...");

		danglingRank = 0;
		/* The number of dangling nodes. */
		long danglingNodes = 0;
		for (long i = n; i-- != 0;) {
			final long o = LongBigArrays.get(outdegree, i);
			if (o == 0 || buckets != null && buckets.getBoolean(i)) {
				danglingRank += DoubleBigArrays.get(rank, i);
				if (LongBigArrays.get(outdegree, i) == 0) danglingNodes++;
			}
		}

		progressLogger.done();
		logger.info(danglingNodes + " dangling nodes");
		if (buckets != null) logger.info(buckets.count() + " buckets");
		logger.info("Initial dangling rank: " + danglingRank);

		normDelta = danglingRankAccumulator = 0;
		completed = false;
		logger.info("Completed.");
		iterationLogger.start();
	}

	private final class IterationThread extends Thread {
		private static final long GRANULARITY = 10000;

		@Override
		public void run() {
			try {
				// We cache frequently used fields.
				final ImmutableGraph graph = PageRankParallelGaussSeidel.this.graph.copy();
				final long n = PageRankParallelGaussSeidel.this.n;
				final double oneMinusAlpha = 1 - alpha;
				final double oneMinusAlphaOverN = oneMinusAlpha / n;
				final double[][] rank = PageRankParallelGaussSeidel.this.rank;
				final long[][] outdegree = PageRankParallelGaussSeidel.this.outdegree;
				final LongArrayBitVector buckets = PageRankParallelGaussSeidel.this.buckets;
				final boolean pseudoRank = PageRankParallelGaussSeidel.this.pseudoRank;
				final double alpha = PageRankParallelGaussSeidel.this.alpha;
				final DoubleBigList danglingNodeDistribution = PageRankParallelGaussSeidel.this.danglingNodeDistribution;
				final DoubleBigList preference = PageRankParallelGaussSeidel.this.preference;
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

						final long end = (Math.min(n, start + GRANULARITY));

						// for each node, enumerate predecessors and compute an updated value
						double danglingRankAccumulator = 0, norm = 0;
						final NodeIterator nodeIterator = graph.nodeIterator(start);

						for (long i = start; i < end; i++) {
							nodeIterator.nextLong();
							s.reset();
							boolean hasLoop = false;

							//Determine the rank from all incoming real links except possibly self link.
							final long[][] pred = nodeIterator.successorBigArray();
							for(long indegree = nodeIterator.outdegree(); indegree-- != 0;) {
								final long currPred = LongBigArrays.get(pred, indegree);
								// Skip buckets
								if (buckets != null && buckets.getBoolean(currPred)) continue;
								if (i == currPred) hasLoop = true;
								else s.add(DoubleBigArrays.get(rank, currPred) / LongBigArrays.get(outdegree, currPred));
							}

							double selfDanglingRank, selfLoopFactor;
							//Determine the diagonal rank contribution
							final long o = LongBigArrays.get(outdegree, i);
							if (o == 0 || buckets != null && buckets.getBoolean(i)) { //i is a dangling node
								selfDanglingRank = DoubleBigArrays.get(rank, i);
								selfLoopFactor = pseudoRank ? 1 :
									(danglingNodeDistribution != null) ? 1 - alpha * danglingNodeDistribution.getDouble(i)
									: 1.0 - alpha / n;
							} else {
								selfDanglingRank = 0;
								selfLoopFactor = hasLoop ? 1 - alpha / o : 1; //i has no selfloop and it is not dangling
							}

							if (! pseudoRank) s.add(danglingNodeDistribution != null ? (danglingRank - selfDanglingRank) * danglingNodeDistribution.getDouble(i) : (danglingRank - selfDanglingRank) / n);

							final double newRank = ((preference != null ? oneMinusAlpha * preference.getDouble(i) : oneMinusAlphaOverN) + alpha * s.value()) / selfLoopFactor;

							if (LongBigArrays.get(outdegree, i) == 0 || buckets != null && buckets.getBoolean(i))
								danglingRankAccumulator += newRank;

							final double r = DoubleBigArrays.get(rank, i);
							if (normVector != null) norm = Math.max(norm, Math.abs(newRank - r) * (1L << (0xFF & ByteBigArrays.get(normVector, i))));
							else norm += Math.abs(newRank - r);

							//update the rank
							DoubleBigArrays.set(rank, i, newRank);
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
			catch(final Throwable t) {
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

		barrier = new CyclicBarrier(numberOfThreads, () -> {
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
		);

		for(int i = thread.length; i-- != 0;) thread[i].start();
		for(int i = thread.length; i-- != 0;)
			try {
				thread[i].join();
			}
			catch (final InterruptedException e) {
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

		final ImmutableGraph graph = mapped? ImmutableGraph.loadMapped(graphBasename, progressLogger) : ImmutableGraph.load(graphBasename, progressLogger);

		DoubleBigList preference = null;
		String preferenceFilename = null;
		if (jsapResult.userSpecified("preferenceVector")) {
			preferenceFilename = jsapResult.getString("preferenceVector");
			final double[][] pref = DoubleBigArrays.newBigArray(graph.numNodes());
			BinIO.loadDoubles(preferenceFilename, pref);
			preference = new DoubleBigArrayBigList(pref);
		}

		if (jsapResult.userSpecified("preferenceObject")) {
			if (jsapResult.userSpecified("preferenceVector")) throw new IllegalArgumentException("You cannot specify twice the preference vector");
			preference = (DoubleBigList)BinIO.loadObject(preferenceFilename = jsapResult.getString("preferenceObject"));
		}

		if (strongly && preference == null) throw new IllegalArgumentException("The 'strongly' option requires a preference vector");

		final PageRankParallelGaussSeidel pr = new PageRankParallelGaussSeidel(graph, threads, LOGGER);
		pr.alpha = jsapResult.getDouble("alpha");
		pr.preference = preference;
		pr.buckets = (LongArrayBitVector)(buckets == null ? null : BinIO.loadObject(buckets));
		pr.stronglyPreferential = strongly;
		pr.pseudoRank = jsapResult.userSpecified("pseudoRank");
		if (normVectorFilename != null) pr.normVector(normVectorFilename, jsapResult.getDouble("sigma"));

		// cycle until we reach maxIter iterations or the norm is less than the given threshold (whichever comes first)
		pr.stepUntil(or(new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(pr.rank, rankBasename + ".ranks");
		pr.buildProperties(graphBasename, preferenceFilename, null).save(rankBasename + ".properties");
	}
}

package it.unimi.dsi.law.rank;

import java.io.IOException;
import java.util.BitSet;

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
 * Copyright (C) 2005-2019 Paolo Boldi, Roberto Posenato, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

// RELEASE-STATUS: DIST

/** Computes PageRank of a graph using the Gau&szlig;&ndash;Seidel method. This class is now mainly of historical
 * interest, as {@link PageRankParallelGaussSeidel} is faster and provides exact bounds via
 * {@linkplain PageRankParallelGaussSeidel#normVector(double[], double) vector norms}.
 *
 * <p>The general formula described in {@link it.unimi.dsi.law.rank.PageRank} can be rewritten as the following linear system:
 * <div style="text-align: center">
 *    <var><b>x</b></var> (<var>I</var> &minus; &alpha; (<var>P</var> + <var><b>u</b></var><sup><i>T</i></sup><var><b>d</b></var>)) =  (1 &minus; &alpha;)<var><b>v</b></var>.
 * </div>
 *
 * <p>The system
 * <div style="text-align: center">
 *  <var><b>x</b></var> <var>M</var> = <var><b>b</b></var>
 * </div>
 * can be solved using the Gauss&minus;Seidel method,
 * which updates in-place a <em>single</em> vector, using the formula
 * <div style="text-align: center">
 * <var>x<sub>i</sub></var><sup>(<var>t</var>+1)</sup> =
 * <big>(</big> <var>b<sub>i</sub></var>
 *       &minus; <big>&Sigma;</big><sub><var>j</var>&lt;<var>i</var></sub> <var>m<sub>ij</sub></var><var>x<sub>j</sub></var><sup>(<var>t</var>+1)</sup>
 *       &minus; <big>&Sigma;</big><sub><var>j</var>&gt;<var>i</var></sub> <var>m<sub>ij</sub></var><var>x<sub>j</sub></var><sup>(<var>t</var>)</sup>
 * <big>)</big>
 * <big>&frasl;</big> <var>m<sub>ii</sub></var>.
 * </div>
 *
 * <p>The {@link #normDelta()} method returns an upper bound to the &#x2113;<sub>1</sub> norm of the error, obtained multiplying by
 * &alpha; / (1 &minus; &alpha;) the &#x2113;<sub>1</sub> norm of the difference between the last two approximations (this idea arose in discussions with David Gleich).
 *
 * <p><strong>Warning</strong>: Since we need to enumerate the <em>predecessors</em> a node,
 * you must pass to the {@linkplain #PageRankGaussSeidel(ImmutableGraph, Logger) constructor} the <strong>transpose</strong>
 * of the graph.
 *
 * <p>Substituting to <var>M</var> and to <var><b>b</b></var> the corresponding terms present in the first formula, we obtain the following update rule:
 * <div style="text-align: center">
 *   <var>x<sub>i</sub></var><sup>(<var>t</var>+1)</sup> =
 *   <big>(</big>
 *     (1 &minus; &alpha;) <var>v<sub>i</sub></var> + &alpha;
 *     <big>(</big>
 *       <big>&Sigma;</big><sub><var>j</var>&lt;<var>i</var></sub> (<var>p<sub>ji</sub></var> + <var>u<sub>i</sub>d<sub>j</sub></var>) <var>x<sub>j</sub></var><sup>(<var>t</var>+1)</sup> +
 *       <big>&Sigma;</big><sub><var>j</var>&gt;<var>i</var></sub> (<var>p<sub>ji</sub></var> + <var>u<sub>i</sub>d<sub>j</sub></var>) <var>x<sub>j</sub></var><sup>(<var>t</var>)</sup>
 *     <big>)</big>
 *   <big>)</big>
 *   <big>&frasl;</big>
 *   (1 &minus; &alpha;<var>p<sub>ii</sub></var> &minus; &alpha;<var>u<sub>i</sub>d<sub>i</sub></var>)
 * </div>
 *
 * <p>We can rearrange the previous formula sums in two different sums: one for the nodes <var>j</var> &rarr; <var>i</var>
 * and one for the dangling nodes (nondangling nodes that are not predecessors of <var>i</var>
 * give no contribution). So the Gau&szlig;&ndash;Seidel method can be implemented as follows:
 * <ul>
 *   <li>initialize <var><b>x</b></var> as the uniform vector;
 *   <li>while some stopping criterion has not been met,
 *   <ul>
 *	   <li>for all <var>i</var> = 0,1,&hellip;, <var>N</var>&minus;1
 *     <ul>
 *       <li>&sigma; = 0;
 *       <li>for all <var>j</var> &rarr; <var>i</var>, with <var>j</var> &#x2260; <var>i</var>, &sigma; += <var>p<sub>ji</sub></var> <var>x<sub>j</sub></var>
 *       <li>for all dangling <var>j</var>, &sigma; += <var>u<sub>i</sub></var> <var>x<sub>j</sub></var>
 *       <li><var>x<sub>i</sub></var> = <big>(</big> (1 &minus; &alpha;) <var>v<sub>i</sub></var> + &alpha;&sigma; <big>)</big>
 *                                      <big>&frasl;</big>
 *                                      (1 &minus; &alpha;<var>p<sub>ii</sub></var> &minus; &alpha;<var>d<sub>i</sub>u<sub>i</sub></var>)
 *     </ul>
 *   </ul>
 * </ul>
 * <p>Remember that <var>u<sub>i</sub></var> is 1&frasl;<var>n</var> when the <em>weakly preferential</em> version of the PageRank formula is considered,
 * <var>v<sub>i</sub></var> when the version is the <em>strongly preferential</em> one.
 *
 * <p>The second &ldquo;for all&rdquo; can be avoided if we keep track of the summation
 * of all ranks of dangling nodes up to index <var>i</var>
 * (exclusive) and for all dangling nodes from index <var>i</var> on in
 * two variables <var>B</var> (<em>before</em> <var>i</var>) and <var>A</var> (<em>after</em> <var>i</var>):
 *<ul>
 *   <li>initialize <var><b>x</b></var> as the uniform vector;
 *   <li><var>B</var> = 0;
 *   <li>for all dangling <var>j</var>, <var>A</var> += <var>x<sub>j</sub></var>;
 *   <li>while some stopping criterion has not been met,
 *   <ul>
 *	   <li>for all <var>i</var> = 0,1,&hellip;,<var>N</var>&minus;1
 *     <ul>
 *       <li>&sigma; = 0;
 *       <li>for all <var>j</var> &rarr; <var>i</var>, with <var>j</var> &#x2260; <var>i</var>, &sigma; += <var>p<sub>ji</sub></var> <var>x<sub>j</sub></var>
 *       <li>&sigma; += (<var>A</var> + <var>B</var> &minus; <var>d<sub>i</sub> x<sub>i</sub></var>) <var>u<sub>i</sub></var>
 *       <li>&sigma; = <big>(</big> (1 &minus; &alpha;) <var>v<sub>i</sub></var> + &alpha;&sigma; <big>)</big>
 *                                      <big>&frasl;</big>
 *                                      (1 &minus; &alpha;<var>p<sub>ii</sub></var> &minus; &alpha;<var>d<sub>i</sub> u<sub>i</sub></var>)
 *		 <li>if <var>i</var> is dangling
 *		 <ul>
 *		   <li><var>B</var> += &sigma;
 *		   <li><var>A</var> &minus;= <var>x<sub>i</sub></var>
 *       </ul>
 *		 <li><var>x<sub>i</sub></var> = &sigma;
 *     </ul>
 *   </ul>
 * </ul>
 *
 *
 * @see PageRank
 * @see SpectralRanking
 *
 * @author Sebastiano Vigna
 */

public class PageRankGaussSeidel extends PageRank {
	/** The class logger */
	private final static Logger LOGGER = LoggerFactory.getLogger(PageRankGaussSeidel.class);

	/** A progress logger monitoring each iteration. */
	private final ProgressLogger progressLogger;
	/** A progress logger monitoring the iterations. */
	private final ProgressLogger iterationLogger;
	/** The amount of ranking in dangling nodes. */
	private double danglingRank;
	/** The norm of the difference vector between the new page rank vector and the previous one. */
	private double norm;
	/** The outdegree of each node of the original graph. */
	private int[] outdegree;

	/** If true, an everywhere zero dangling-node distribution will be simulated, resulting in the computation of a pseudorank. */
	public boolean pseudoRank;

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute PageRank.
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	protected PageRankGaussSeidel(final ImmutableGraph transpose, final Logger logger) {
		super(transpose, logger);
		progressLogger = new ProgressLogger(logger, "nodes");
		iterationLogger = new ProgressLogger(logger, "iterations");
	}

	/** Creates a new instance.
	 *
	 * @param transpose the transpose of the graph on which to compute PageRank.
	 */
	public PageRankGaussSeidel(final ImmutableGraph transpose) {
		this(transpose, LOGGER);
	}

	@Override
	public void init() throws IOException {
		super.init();

		if (outdegree == null) {
			// We allocate and compute the outdegree vector.
			outdegree = new int[n];

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
		logger.info("Completed.");
		iterationLogger.start();
	}

	@Override
	public void clear() {
		super.clear();
		outdegree = null;
	}
	/** Return an upper bound to the &#x2113;<sub>1</sub> norm of the error, obtained multiplying by
	 * &alpha; / (1 &minus; &alpha;) the &#x2113;<sub>1</sub> norm of the difference between the last two approximations (this idea arose in discussions with David Gleich).
	 *
	 * @return an upper bound to the &#x2113;<sub>1</sub> norm of the error.
	 */
	@Override
	public double normDelta() {
		return norm * alpha / (1 - alpha);
	}


	public void step() {
		final double oneMinusAlpha = 1.0 - alpha,
			oneMinusAlphaOverNumNodes = oneMinusAlpha / n;
		final NodeIterator nodeIterator = graph.nodeIterator();
		int inDegree, currPred;
		int[] pred;
		double sigma,
			selfLoopFactor,
			selfDanglingRank,
			B = 0, // See class description
			A = danglingRank;
		boolean hasLoop;
		final double[] rank = this.rank;
		final int[] outdegree = this.outdegree;
		final BitSet buckets = this.buckets;
		final boolean pseudoRank = this.pseudoRank;
		final double alpha = this.alpha;
		final DoubleList danglingNodeDistribution = this.danglingNodeDistribution;
		final DoubleList preference = this.preference;
		final int n = this.n;

		progressLogger.expectedUpdates = n;
		progressLogger.start("Iteration " + iteration++ + "...");

		norm = 0;
		for(int i = 0; i < n; i++) {
			nodeIterator.nextInt();
			inDegree = nodeIterator.outdegree();
			pred = nodeIterator.successorArray();
			sigma = 0.0;
			hasLoop = false;

			//Determine the rank from all incoming real links except possibly self link.
			for (int j = inDegree; j-- != 0;) {
				currPred = pred[j];
				// Skip buckets
				if (buckets != null && buckets.get(pred[j])) continue;
				if (i == currPred) hasLoop = true;
				else sigma += rank[currPred] / outdegree[currPred];
			}

			//Determine the diagonal rank contribution
			if (outdegree[i] == 0 || buckets != null && buckets.get(i)) { //i is a dangling node
				selfDanglingRank = rank[i];
				selfLoopFactor = pseudoRank ? 1.0 :
					(danglingNodeDistribution != null) ? 1.0 - alpha * danglingNodeDistribution.getDouble(i)
					: 1.0 - alpha / n;
			} else {
				selfDanglingRank = 0.0;
				selfLoopFactor = (hasLoop) ? 1.0 - alpha / outdegree[i]
					: 1.0; //i has no selfloop and it is not dangling
			}

			sigma += pseudoRank ? 0 :
				(danglingNodeDistribution != null) ? (B + A - selfDanglingRank) * danglingNodeDistribution.getDouble(i)
				: (B + A - selfDanglingRank) / n;

			sigma = (preference != null)	? (oneMinusAlpha * preference.getDouble(i) + alpha * sigma) / selfLoopFactor
				: (oneMinusAlphaOverNumNodes + alpha * sigma) / selfLoopFactor;

			if (outdegree[i] == 0 || buckets != null && buckets.get(i)) {
				B += sigma;
				A -= rank[i];
			}

			//update the L_1 norm of vector difference between the new and old rank
			norm += Math.abs(sigma - rank[i]);
			//update the rank
			rank[i] = sigma;

			progressLogger.update();
		}
		danglingRank = B;
		progressLogger.done();
		iterationLogger.setAndDisplay(iteration);
	}

	@Override
	public void stepUntil(final StoppingCriterion stoppingCriterion) throws IOException {
		super.stepUntil(stoppingCriterion);
		iterationLogger.done();
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException, ClassNotFoundException {

		final SimpleJSAP jsap = new SimpleJSAP(PageRankGaussSeidel.class.getName(), "Computes PageRank of a graph, given its transpose, using Gauss-Seidel's method."
				+ " The file <rankBasename>.properties stores metadata about the computation, whereas the file <rankBasename>.ranks stores the result as a sequence of doubles in DataInput format.",
			new Parameter[] {
			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, Double.toString(PageRank.DEFAULT_ALPHA), JSAP.NOT_REQUIRED, 'a', "alpha", "Damping factor."),
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "Threshold in l_1 norm to determine whether to stop."),
			new FlaggedOption("preferenceVector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "preference-vector", "A preference vector stored as a vector of binary doubles."),
			new FlaggedOption("preferenceObject", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'P', "preference-object", "A preference vector stored as a serialised DoubleList."),
			new Switch("pseudoRank", JSAP.NO_SHORTFLAG, "pseudorank", "Compute pseudoranks (the dangling preference is set to 0)."),
			new FlaggedOption("buckets", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "buckets", "The buckets of the graph; if supplied, buckets will be treated as dangling nodes."),
			new Switch("offline", 'o', "offline", "No-op for compatibility."),
			new Switch("strongly", 'S', "strongly", "Use the preference vector to redistribute the dangling rank."),
			new UnflaggedOption("transposeBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename where the results are stored. <rankBasename>.properties contains the parameter values used in the computation. <rankBasename>.ranks contains ranks (doubles in binary form).")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean strongly = jsapResult.getBoolean("strongly", false);
		final String buckets = jsapResult.getString("buckets");
		final String graphBasename = jsapResult.getString("transposeBasename");
		final String rankBasename = jsapResult.getString("rankBasename");

		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");
		ImmutableGraph graph = null;

		graph = ImmutableGraph.loadOffline(graphBasename, progressLogger);

		DoubleList preference = null;
		String preferenceFilename = null;
		if (jsapResult.userSpecified("preferenceVector"))
			preference = DoubleArrayList.wrap(BinIO.loadDoubles(preferenceFilename = jsapResult.getString("preferenceVector")));

		if (jsapResult.userSpecified("preferenceObject")) {
			if (jsapResult.userSpecified("preferenceVector")) throw new IllegalArgumentException("You cannot specify twice the preference vector");
			preference = (DoubleList)BinIO.loadObject(preferenceFilename = jsapResult.getString("preferenceObject"));
		}

		if (strongly && preference == null) throw new IllegalArgumentException("The 'strongly' option requires a preference vector");

		PageRankGaussSeidel pr = new PageRankGaussSeidel(graph);
		pr.alpha = jsapResult.getDouble("alpha");
		pr.preference = preference;
		pr.buckets = (BitSet)(buckets == null ? null : BinIO.loadObject(buckets));
		pr.stronglyPreferential = strongly;
		pr.pseudoRank = jsapResult.getBoolean("pseudoRank");

		// cycle until we reach maxIter interations or the norm is less than the given threshold (whichever comes first)
		pr.stepUntil(or(new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(pr.rank, rankBasename + ".ranks");
		pr.buildProperties(graphBasename, preferenceFilename, null).save(rankBasename + ".properties");
	}
}

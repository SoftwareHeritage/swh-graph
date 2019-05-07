package it.unimi.dsi.law.rank;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
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
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

// RELEASE-STATUS: DIST

/** Computes PageRank (and possibly its derivatives in the damping factor) using its power series.
 *
 * <P>PageRank has a power series expansion in &alpha; (see {@link PageRank} for definitions):
 *
 * <div style="text-align: center; margin: 1em">
 * <var><b>r</b></var> = <var><b>v</b></var> +
 * <var><b>v</b></var><big>&Sigma;</big><sub><var>n</var>&ge;<var>1</var></sub>&alpha;<sup><var>n</var></sup><big>(</big> (<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n</var></sup> &minus; (<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n&minus;1</var></sup> <big>)</big>
 * </div>
 *
 * <p>In &ldquo;PageRank: Functional dependencies&rdquo;, by  Paolo Boldi, Massimo Santini, and Sebastiano Vigna,
 * <i>ACM Trans. Inf. Sys.</i>, 27(4):1&minus;23, 2009, we show that the truncation of order <var>k</var> of the power series
 * is exactly the value attained by the power method using <var><b>v</b></var> as starting vector. This class exploits the equivalence to compute iteratively the Maclaurin polynomials
 * of order <var>k</var>:
 *
 * <div style="text-align: center">
 * <var><b>r</b></var><sup>(<var>k</var>)</sup> =
 * <var><b>v</b></var> <big>(</big> &alpha; <var>P</var> + &alpha; <var><b>d</b><sup>T</sup></var><var><b>u</b></var> +  (1&minus;&alpha;)<b>1</b><sup><var>T</var></sup> <var><b>v</b></var> <big>)</big><sup><var>k</var></sup> =
 * <var><b>v</b></var> +
 * <var><b>v</b></var><big>&Sigma;</big><sub>1&le;<var>n</var>&le;<var>k</var></sub> &alpha;<sup><var>n</var></sup><big>(</big> (<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n</var></sup> &minus; (<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n&minus;1</var></sup> <big>)</big>.
 * </div>
 *
 * <p>The remainder (i.e., the difference with PageRank) at the <var>k</var>-th iteration can be bounded exactly
 * using the norm of the difference <var><b>r</b></var><sup>(<var>k</var>)</sup>&nbsp;&minus;&nbsp;<var><b>r</b></var><sup>(<var>k</var>&minus;1)</sup>, as
 * <div style="text-align: center; margin: 1em">
 * <var><b>r</b></var> &minus; <var><b>r</b></var><sup>(<var>k</var>)</sup>
 * = <var><b>v</b></var><big>&Sigma;</big><sub><var>n</var>&ge;<var>k</var>+1</sub> &alpha;<sup><var>n</var></sup><big>(</big> (<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n</var></sup> &minus; (<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n&minus;1</var></sup> <big>)</big>
 *  =  <var><b>v</b></var> &alpha;<sup><var>k</var></sup><big>(</big> (<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>k</var></sup>
 *  &minus; (<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>k&minus;1</var></sup> <big>)</big> <big>&Sigma;</big><sub><var>n</var>&ge;1</sub>&alpha;<sup><var>n</var></sup>(<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n</var></sup>
 *  = (<var><b>r</b></var><sup>(<var>k</var>)</sup>&nbsp;&minus;&nbsp;<var><b>r</b></var><sup>(<var>k</var>&minus;1)</sup>)  <big>&Sigma;</big><sub><var>n</var>&ge;1</sub>&alpha;<sup><var>n</var></sup>(<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n</var></sup>.
 * </div>
 *
 * <p>Hence,
 * <div style="text-align: center; margin: 1em">
 * &#x2016;<var><b>r</b></var> &minus; <var><b>r</b></var><sup>(<var>k</var>)</sup>&#x2016;<sub>1</sub> &le;
 *
 * &#x2016;<var><b>r</b></var><sup>(<var>k</var>)</sup>&nbsp;&minus;&nbsp;<var><b>r</b></var><sup>(<var>k</var>&minus;1)</sup>&#x2016;<sub>1</sub> &#x2016;<big>&Sigma;</big><sub><var>n</var>&ge;1</sub>&alpha;<sup><var>n</var></sup>(<var>P</var> + <var><b>d</b></var><sup><var>T</var></sup><var><b>u</b></var>)<sup><var>n</var></sup>&#x2016;<sub>1</sub>
 *  &le; &#x2016;<var><b>r</b><sup>(k)</sup></var>&nbsp;&minus;&nbsp;<var><b>r</b></var><sup>(<var>k</var>&minus;1)</sup>&#x2016;<sub>1</sub> &alpha; / (1 &minus; &alpha;),
 * </div>
 * and this is the value returned by {@link #normDelta()}.
 *
 * <p>It is worth remarking that a folklore justification for convergence, that is, the spectral gap between the dominant eigenvalue and the second eigenvalue, does <em>not</em> apply
 * directly to PageRank, as it is applicable only to normal (i.e., diagonalizable) matrices. The inequality above, instead, makes it possible to bound in a precise manner the
 * error in the current estimation.
 *
 * <p>Note that
 *  <div style="text-align: center; margin: 1em">
 * <var><b>r</b></var><sup>(<var>t</var>+1)</sup> =
 * <var><b>r</b></var><sup>(<var>t</var>)</sup> (&alpha; <var>P</var> + &alpha; <var><b>d</b><sup>T</sup></var><var><b>u</b></var> +  (1&minus;&alpha;)<b>1</b><sup><var>T</var></sup> <var><b>v</b></var>) =
 * &alpha;<var><b>r</b></var><sup>(<var>t</var>)</sup> <var>P</var> + &alpha; <var><b>r</b></var><sup>(<var>t</var>)</sup><var><b>d</b></var> <var><b>u</b></var> + (1&minus;&alpha;) <var><b>v</b></var>.
 * </div>
 *
 * <p>The latter formula means that
 * <div style="text-align: center; margin: 1em">
 * <var>r<sub>i</sub></var><sup>(<var>t</var>+1)</sup>=
 * &alpha;<big>&Sigma;</big><sub><var>j</var> &rarr; <var>i</var></sub> <var>p<sub>ji</sub></var><var>r<sub>j</sub></var><sup>(<var>t</var>)</sup>
 * + &alpha;&kappa;<var>u</var><sub><var>i</var></sub>
 * + (1&minus;&alpha;)<var>v</var><sub><var>i</var></sub>,
 * </div>
 * where &kappa; is the sum of <var>r<sub>j</sub></var><sup>(<var>t</var>)</sup>
 * over all dangling nodes. This is the formula used in the code.
 *
 * <P>The attribute {@link #previousRank} represents the ranking at the previous step.
 *
 * <h2>Derivatives and coefficients of the Maclaurin polynomials</h2>
 *
 * <p>Using results from &ldquo;PageRank: Functional dependencies&rdquo;, by  Paolo Boldi, Massimo Santini, and Sebastiano Vigna,
 * <i>ACM Trans. Inf. Sys.</i>, 27(4):1&minus;23, 2009, this class is able also to approximate the derivatives
 * of PageRank in {@linkplain #alpha &alpha;}, and to compute, for each node, the
 * coefficients of Maclaurin polynomials. You have to set a non-empty {@link #order} array specifying
 * the order of the derivatives desired, or a {@linkplain #coeffBasename basename for the coefficients},
 * respectively . The derivatives will be evaluated (as PageRank is) in the value set for &alpha;.
 *
 * @see PageRank
 * @see SpectralRanking
 */

public class PageRankPowerSeries extends PageRank {
	private final static Logger LOGGER = LoggerFactory.getLogger(PageRankPowerSeries.class);

	/** The rank vector after the last iteration (only meaningful after at least one step). */
	public double[] previousRank;
	/** A progress logger monitoring each iteration. */
	private final ProgressLogger progressLogger;
	/** A progress logger monitoring the iterations. */
	private final ProgressLogger iterationLogger;
	/** If not {@code null}, the subset of nodes over which the derivatives should be computed. */
	public int[] subset;
	/** The value of derivatives  (only for the subset of nodes specified in {@link #subset}, if not {@code null}). */
	public double[][] derivative;
	/** The order of the derivatives. Must be non-{@code null}, but it can be the empty array. */
	public int[] order = IntArrays.EMPTY_ARRAY;
	/** If not {@code null}, the basename for coefficents. */
	public String coeffBasename;

	/** Creates a new instance.
	 *
	 * @param graph the graph.
	 * @param logger a logger that will be passed to <code>super()</code>.
	 */
	public PageRankPowerSeries(final ImmutableGraph graph, final Logger logger) {
		super(graph, logger);
		progressLogger = new ProgressLogger(logger, "nodes");
		iterationLogger = new ProgressLogger(logger, "iterations");
	}

	/** Creates a new instance.
	 *
	 * @param graph the graph.
	 */
	public PageRankPowerSeries(final ImmutableGraph graph) {
		this(graph, LOGGER);
	}

	@Override
	public void init() throws IOException {
		super.init();
		// Creates the arrays, if necessary
		if (previousRank == null) previousRank = new double[n];
		derivative = new double[order.length][subset != null ? subset.length : n];
		if (IntArrayList.wrap(order).indexOf(0) != -1) throw new IllegalArgumentException("You cannot compute the derivative of order 0 (use PageRank instead)");
		if (coeffBasename != null) BinIO.storeDoubles(rank, coeffBasename + "-" + 0);

		logger.info("Completed.");
		iterationLogger.start();
	}

	@Override
	public void step() throws IOException {
		final double[] oldRank = rank, newRank = previousRank;
		Arrays.fill(newRank, 0);

		// for each node, calculate its outdegree and redistribute its rank among pointed nodes
		double accum = 0.0;

		progressLogger.expectedUpdates = n;
		progressLogger.start("Iteration " + iteration++ + "...");

		final NodeIterator nodeIterator = graph.nodeIterator();
		int outdegree;
		int[] succ;

		for(int i = 0; i < n; i++) {
			nodeIterator.nextInt();
			outdegree = nodeIterator.outdegree();

			if (outdegree == 0 || buckets != null && buckets.get(i)) accum += oldRank[i];
			else {
				int j = outdegree;
				succ = nodeIterator.successorArray();
				while (j-- != 0) newRank[succ[j]] += oldRank[i] / outdegree;
			}
			progressLogger.update();
		}
		progressLogger.done();

		final double accumOverNumNodes = accum / n;

		final double oneOverNumNodes = 1.0 / n;
		if (preference != null)
			if (danglingNodeDistribution == null)
				for(int i = n; i-- != 0;) newRank[i] = alpha * newRank[i] + (1 - alpha) * preference.getDouble(i) + alpha * accumOverNumNodes;
			else
				for(int i = n; i-- != 0;) newRank[i] = alpha * newRank[i] + (1 - alpha) * preference.getDouble(i) + alpha * accum * danglingNodeDistribution.getDouble(i);
		else
			if (danglingNodeDistribution == null)
				for(int i = n; i-- != 0;) newRank[i] = alpha * newRank[i] + (1 - alpha) * oneOverNumNodes + alpha * accumOverNumNodes;
			else
				for(int i = n; i-- != 0;) newRank[i] = alpha * newRank[i] + (1 - alpha) * oneOverNumNodes + alpha * accum * danglingNodeDistribution.getDouble(i);

		//make the rank just computed the new rank
		rank = newRank;
		previousRank = oldRank;

		// Compute derivatives.
		if (subset == null) {
			for(int i = 0; i < order.length; i++) {
				final int k = order[i];
				final double alphak = Math.pow(alpha, k);
				final double nFallingK = it.unimi.dsi.law.Util.falling(iteration, k);
				for(int j = 0; j < n; j++) derivative[i][j] += nFallingK * (rank[j] - previousRank[j]) / alphak;
			}
		}
		else {
			for(int i = 0; i < order.length; i++) {
				final int k = order[i];
				final double alphak = Math.pow(alpha, k);
				final double nFallingK = it.unimi.dsi.law.Util.falling(iteration, k);

				for(final int t: subset) derivative[i][t] += nFallingK * (rank[t] - previousRank[t]) / alphak;
			}
		}

		// Compute coefficients, if required.

		if (coeffBasename != null) {
			final DataOutputStream coefficients = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(coeffBasename + "-" + (iteration))));
			final double alphaN = Math.pow(alpha, iteration);
			for(int i = 0; i < n; i++) coefficients.writeDouble((rank[i] - previousRank[i]) / alphaN);
			coefficients.close();
		}

		iterationLogger.setAndDisplay(iteration);
	}

	@Override
	public void stepUntil(final StoppingCriterion stoppingCriterion) throws IOException {
		super.stepUntil(stoppingCriterion);

		for(int i = 0; i < order.length; i++) {
			if (iteration < order[i] / (1 - alpha)) LOGGER.info("Error bound for derivative of order " + order[i] + " (alpha=" + alpha + "): unknown");
			else {
				final int k = order[i];
				final double delta = alpha * iteration / (iteration + k);
				final double alphak = Math.pow(alpha, k);
				final double nFallingK = it.unimi.dsi.law.Util.falling(iteration, k);
				double infinityNorm = 0;
				for(int j = 0; j < n; j++) infinityNorm = Math.max(infinityNorm, nFallingK * (rank[j] - previousRank[j]) / alphak);

				LOGGER.info("Error bound for derivative of order " + k + " (alpha=" + alpha + "): " + infinityNorm * delta / (1 - delta));
			}
		}

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
		derivative = null;
	}

	public static void main(final String[] arg) throws IOException, JSAPException, ConfigurationException, ClassNotFoundException {

		final SimpleJSAP jsap = new SimpleJSAP(PageRankPowerSeries.class.getName(), "Computes PageRank of a graph using the power-series method. Additionally, computes derivatives and coefficients of Maclaurin polynomials."
			+ " The file <rankBasename>.properties stores metadata about the computation, whereas the file <rankBasename>.ranks stores the result as a sequence of doubles in DataInput format.",
			new Parameter[] {
			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, Double.toString(PageRank.DEFAULT_ALPHA), JSAP.NOT_REQUIRED, 'a', "alpha", "Damping factor."),
			new FlaggedOption("maxIter", JSAP.INTEGER_PARSER, Integer.toString(DEFAULT_MAX_ITER), JSAP.NOT_REQUIRED, 'i', "max-iter", "Maximum number of iterations."),
			new FlaggedOption("threshold", JSAP.DOUBLE_PARSER, Double.toString(DEFAULT_THRESHOLD), JSAP.NOT_REQUIRED, 't', "threshold", "Threshold to determine whether to stop."),
			new FlaggedOption("coeff", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "coeff", "Save the k-th coefficient of the Maclaurin polynomial using this basename."),
			new FlaggedOption("derivative", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "derivative", "The order(s) of the derivative(s) to be computed (>0).").setAllowMultipleDeclarations(true),
			new FlaggedOption("preferenceVector", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'p', "preference-vector", "A preference vector stored as a vector of binary doubles."),
			new FlaggedOption("preferenceObject", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'P', "preference-object", "A preference vector stored as a serialised DoubleList."),
			new FlaggedOption("buckets", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'b', "buckets", "The buckets of the graph; if supplied, buckets will be treated as dangling nodes."),
			new Switch("offline", 'o', "offline", "No-op for compatibility."),
			new Switch("strongly", 'S', "strongly", "use the preference vector to redistribute the dangling rank."),
			new UnflaggedOption("graphBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean strongly = jsapResult.getBoolean("strongly", false);
		final int[] order = jsapResult.getIntArray("derivative");
		final String graphBasename = jsapResult.getString("graphBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final String buckets = jsapResult.getString("buckets");
		final String coeffBasename = jsapResult.getString("coeff");
		final ProgressLogger progressLogger = new ProgressLogger(LOGGER, "nodes");

		final ImmutableGraph graph = ImmutableGraph.loadOffline(graphBasename, progressLogger);

		DoubleList preference = null;
		String preferenceFilename = null;
		if (jsapResult.userSpecified("preferenceVector"))
			preference = DoubleArrayList.wrap(BinIO.loadDoubles(preferenceFilename = jsapResult.getString("preferenceVector")));

		if (jsapResult.userSpecified("preferenceObject")) {
			if (jsapResult.userSpecified("preferenceVector")) throw new IllegalArgumentException("You cannot specify twice the preference vector");
			preference = (DoubleList)BinIO.loadObject(preferenceFilename = jsapResult.getString("preferenceObject"));
		}

		if (strongly && preference == null) throw new IllegalArgumentException("The 'strongly' option requires a preference vector");

		final PageRankPowerSeries pr = new PageRankPowerSeries(graph);
		pr.alpha = jsapResult.getDouble("alpha");
		pr.preference = preference;
		pr.buckets = (BitSet)(buckets == null ? null : BinIO.loadObject(buckets));
		pr.stronglyPreferential = strongly;
		pr.order = order != null ? order : null;
		pr.coeffBasename = coeffBasename;

		// cycle until we reach maxIter iterations or the norm is less than the given threshold (whichever comes first)
		pr.stepUntil(or(new SpectralRanking.NormStoppingCriterion(jsapResult.getDouble("threshold")), new SpectralRanking.IterationNumberStoppingCriterion(jsapResult.getInt("maxIter"))));

		BinIO.storeDoubles(pr.rank, rankBasename + ".ranks");
		pr.buildProperties(graphBasename, preferenceFilename, null).save(rankBasename + ".properties");

		if (order != null) for(int i = 0; i < order.length; i++) BinIO.storeDoubles(pr.derivative[i], rankBasename + ".der-" + order[i]);
	}
}

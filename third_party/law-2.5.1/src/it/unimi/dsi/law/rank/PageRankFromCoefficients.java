package it.unimi.dsi.law.rank;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

/*
 * Copyright (C) 2007-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.logging.ProgressLogger;

// RELEASE-STATUS: DIST

/** Computes PageRank using its power series.
 *
 * <p>This class uses the vectors of coefficients
 * of the PageRank power series computed optionally by {@link PageRankPowerSeries}
 * to approximate PageRank and any of its derivatives for a given value of the damping factor &alpha;.
 * The computation is based on the results
 * described in &ldquo;PageRank: Functional dependencies&rdquo;, by  Paolo Boldi, Massimo Santini, and Sebastiano Vigna,
 * <i>ACM Trans. Inf. Sys.</i>, 27(4):1&minus;23, 2009.
 */

public class PageRankFromCoefficients {
	private final static Logger LOGGER = LoggerFactory.getLogger(PageRankFromCoefficients.class);

	private final static int BUFFER_SIZE = 1024 * 1024;

	/** Computes PageRank and its derivatives for given damping factor values.
	 *
	 * <p>For each pair of damping factor and derivative order passed to this method, a
	 * suitable rank file will be generated. Information about the precision guarantee will be logged.
	 *
	 * @param coeffBasename the basename of coefficient files (as computed by {@link PageRankPowerSeries}); the
	 * actual names are obtained appending <code>-<var>i</var></code> for the <var>i</var>-th coefficient.
	 * @param numCoeff the number of coefficient files.
	 * @param rankBasename the basename of the computed ranks.
	 * @param alpha an array of values of the damping factor, one for each rank to compute.
	 * @param order an array of derivative orders (0 means PageRank), parallel to <code>alpha</code>.
	 */

	public static void compute(final String coeffBasename, final int numCoeff, final String rankBasename, final double[] alpha, final int[] order) throws IOException {
		LOGGER.info("Opening files...");
		final int numDerivatives = order.length;
		final int numAlphas = alpha.length;

		final DoubleIterator[] coeff = new DoubleIterator[numCoeff];
		final DataInputStream[] coeffStream = new DataInputStream[numCoeff];
		for(int i = numCoeff; i-- != 0;) coeff[i] = BinIO.asDoubleIterator(coeffStream[i] = new DataInputStream(new FastBufferedInputStream(new FileInputStream(coeffBasename + "-" + i), BUFFER_SIZE)));
		final DataOutputStream[][] result = new DataOutputStream[numAlphas][numDerivatives];
		for(int l = numAlphas; l-- != 0;)
			for(int i = numDerivatives; i-- != 0;)
				result[l][i] = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(rankBasename + "-" + alpha[l] + (order[i] == 0 ? ".ranks" : ".der-" + order[i])), BUFFER_SIZE));


		LOGGER.info("Computing coefficients...");

		// a[i][n] is n falling order[i] * alpha^n
		final double[][][] a = new double[numAlphas][numDerivatives][numCoeff];
		for(int i = numDerivatives; i-- != 0;) {
			final int k = order[i];
			for(int l = numAlphas; l-- != 0;) {
				double alphaNMinusK = 1;
				a[l][i][0] = it.unimi.dsi.law.Util.falling(0, k);
				for(int n = 1; n < numCoeff; n++) {
					// Note that until n >= k fallink(n, k) = 0, so the value of alphanminusk is irrelevant.
					if (n > k) alphaNMinusK *= alpha[l];
					a[l][i][n] = alphaNMinusK * it.unimi.dsi.law.Util.falling(n, k);
				}

				//System.err.println("Order " + order[i] + ": " + Arrays.toString(a[i]));
			}
		}


		final int numNodes = (int)(new File(coeffBasename + "-0").length() / (Double.SIZE / 8));
		final double[][] partialSum = new double[numAlphas][numDerivatives];
		final double[][] infinityNorm = new double[numAlphas][numDerivatives];

		final ProgressLogger pl = new ProgressLogger(LOGGER);
		pl.itemsName = "nodes";
		pl.expectedUpdates = numNodes;

		pl.start("Computing PageRank and derivatives...");
		for(int i = 0; i < numNodes; i++) {
			for(int l = numAlphas; l-- != 0;) Arrays.fill(partialSum[l], 0);
			double t = 0;

			for(int n = 0; n < numCoeff; n++) {
				t = coeff[n].nextDouble();
				for(int l = numAlphas; l-- != 0;)
					for(int j = numDerivatives; j-- != 0;)
						partialSum[l][j] += a[l][j][n] * t;
			}

			for(int l = numAlphas; l-- != 0;)
				for(int j = numDerivatives; j-- != 0;) {
					infinityNorm[l][j] = Math.max(infinityNorm[l][j], a[l][j][numCoeff - 1] * t);
					result[l][j].writeDouble(partialSum[l][j]);
				}

			pl.update();
		}

		pl.done();

		for(int i = 0; i < numCoeff; i++) coeffStream[i].close();
		for(int l = numAlphas; l-- != 0;) {
			for(int i = 0; i < numDerivatives; i++) {
				if (numCoeff - 1 < order[i] / (1 - alpha[l])) LOGGER.info("Error bound for derivative of order " + order[i] + " (alpha=" + alpha[l] + "): unknown");
				else {
					final double delta = alpha[l] * (numCoeff - 1) / (numCoeff - 1 + order[i]);
					LOGGER.info("Error bound for derivative of order " + order[i] + " (alpha=" + alpha[l] + "): " + infinityNorm[l][i] * delta / (1 - delta));
				}
				result[l][i].close();
			}
		}
	}

	public static void main(final String[] arg) throws IOException, JSAPException {

		final SimpleJSAP jsap = new SimpleJSAP(PageRankFromCoefficients.class.getName(),
				"Computes PageRank and possibly its derivatives using the coefficients of the PageRank power series (usually computed by PageRankPowerSeries).",
			new Parameter[] {
			new FlaggedOption("alpha", JSAP.DOUBLE_PARSER, Double.toString(PageRank.DEFAULT_ALPHA), JSAP.NOT_REQUIRED, 'a', "alpha", "Damping factor(s), one for each desired output.").setAllowMultipleDeclarations(true),
			new FlaggedOption("numCoeff", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'n', "num-coeff", "The number of coefficients to use."),
			new FlaggedOption("derivative", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "derivative", "The order(s) of the derivative(s) to be computed (0 means PageRank); the orders are interpreted as a list of specifications parallel to that of the damping factors.").setAllowMultipleDeclarations(true),
			new UnflaggedOption("coeffBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the coefficients."),
			new UnflaggedOption("rankBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The filename where the resulting rank (doubles in binary form) are stored.")
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String coeffBasename = jsapResult.getString("coeffBasename");
		final String rankBasename = jsapResult.getString("rankBasename");
		final double[] alpha = jsapResult.getDoubleArray("alpha");
		final int[] order = jsapResult.getIntArray("derivative");
		final int numCoeff = jsapResult.getInt("numCoeff");

		compute(coeffBasename, numCoeff, rankBasename, alpha, order);
	}
}

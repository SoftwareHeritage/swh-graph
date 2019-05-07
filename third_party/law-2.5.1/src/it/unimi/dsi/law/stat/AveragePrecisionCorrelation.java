package it.unimi.dsi.law.stat;

import java.io.IOException;

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

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.law.util.Precision;

// RELEASE-STATUS: DIST

/** Computes the AP (average-precision) correlation between two score vectors without ties. More precisely,
 * this class computes the formula given by
 * Emine Yilmaz, Javed A. Aslam, and Stephen Robertson in &ldquo;A new rank correlation coefficient for information retrieval&rdquo;,
 * <i>Proc. of the 31st annual international ACM SIGIR conference on Research and development in information retrieval</i>,
 * pages 587&minus;594, ACM, 2008,
 *  using the algorithm described by
 * Sebastiano Vigna in &ldquo;<a href="http://vigna.di.unimi.it/papers.php#VigWCIRT">A weighted correlation index for rankings
 * with ties</a>&rdquo;, 2014.
 *
 * <p>This class is a singleton: methods must be invoked on {@link #INSTANCE}.
 * Additional methods inherited from {@link CorrelationIndex} make it possible to
 * compute directly AP correlation bewteen two files, to bound the number of significant digits, or
 * to reverse the standard association between scores and ranks (by default,
 * a larger score corresponds to a higher rank, i.e., to a smaller rank index; the largest score gets
 * rank 0).
 *
 * <p>A main method is provided for command-line usage.
 */

public class AveragePrecisionCorrelation extends CorrelationIndex {
	private AveragePrecisionCorrelation() {}

	/** The singleton instance of this class. */
	public static final AveragePrecisionCorrelation INSTANCE = new AveragePrecisionCorrelation();

	private static final class ExchangeWeigher {
		/** A support array used by MergeSort. */
		private final int[] temp;
		/** The first score vector. */
		private final double[] v0;
		/** An array of integers, initially sorted by the second score vector. */
		private final int perm[];
		/** The inverse of {@link #perm}.*/
		private final int[] rank;

		public ExchangeWeigher(final double v0[], double[] v1) {
			final int length = v0.length;
			this.v0 = v0;
			this.temp = new int[length];
			perm = Util.identity(length);
			// First of all we sort perm stably by the first rank vector (higher ranks come first!).
			DoubleArrays.radixSortIndirect(perm, v1, true);
			IntArrays.reverse(perm);
			// We null v1 so that the garbage collector can reclaim it.
			v1 = null;
			rank = Util.invertPermutation(perm);
		}

		public double weigh() {
			return weigh(0, perm.length);
		}

		/** Orders an array fragment of {@link #perm} and returns the weight of the necessary exchanges.
		 *
		 * @param offset the starting element of the array fragment.
		 * @param length the number of elements of the array fragment.
		 * @return the weight of exchanges used to sort the array fragment.
		 */
		private double weigh(final int offset, final int length) {
			/* Using a non-recursive sort for small subarrays gives no noticeable
			 * improvement, as most of the cost is given by floating-point computations. */
			if (length == 1) return 0;

			final int length0 = length / 2, length1 = length - length / 2, middle = offset + length0;
			double weight = weigh(offset, length0);
			weight += weigh(middle, length1);

			/* If the last element of the first subarray is larger than or equal to the first element of
			 * the second subarray, there is nothing to do. */
			if (v0[perm[middle - 1]] < v0[perm[middle]]) {
				// We merge the lists into temp, adding the number of forward moves to concordances.
				int i = 0, j = 0, k = 0;
				while(j < length0 && k < length1) {
					//System.err.println("j: " + j + " k: " + k + " " + v1[perm[offset + j]] + " <> " + v1[perm[middle + k]]);
					if (v0[perm[offset + j]] > v0[perm[middle + k]]) {
						//System.err.println(v1[perm[offset + j]] + " > " + v1[perm[middle + k]] + " -> "+residual);
						temp[i] = perm[offset + j++];
					}
					else {
						temp[i] = perm[middle + k++];
						weight += (length0 - j) * (1. / rank[temp[i]]);
					}
					i++;
				}

				System.arraycopy(perm, offset + j, perm, offset + i, length0 - j);
				System.arraycopy(temp, 0, perm, offset, i);
			}

			return weight;
		}
	}


	/** Computes AP correlation between two score vectors.
	 *
	 * <p>Note that this method must be called with some care. More precisely, the two
	 * arguments should be built on-the-fly in the method call, and not stored in variables,
	 * as the first argument array will be {@code null}'d during the execution of this method
	 * to free some memory: if the array is referenced elsewhere the garbage collector will not
	 * be able to collect it.
	 *
	 * @param v0 the first score vector.
	 * @param v1 the second score vector (inducing the reference ranking).
     * @return AP correlation.
	 */
	public double compute(double v0[], final double v1[]) {
		if (v0.length != v1.length) throw new IllegalArgumentException("Array lengths differ: " + v0.length + ", " + v1.length);
		final int length = v0.length;
		if (length == 0) throw new IllegalArgumentException("AP correlation is undefined on empty score vectors");

		final double e = new ExchangeWeigher(v0, v1).weigh() / (length - 1);

		// Ensure interval [-1..1] (small deviations might appear because of numerical errors).
		return Math.min(1, Math.max(-1, (1 - 2 * e)));
	}

	public static void main(String[] arg) throws NumberFormatException, IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(AveragePrecisionCorrelation.class.getName(),
			"Computes the AP correlation between the score vectors contained in two given files. " +
			"The two files must contain the same number of doubles, written " +
			"in Java binary format. The option -t makes it possible to specify a different " +
			"type (possibly for each input file)." +
			"\n" +
			"If one or more truncations are specified with the option -T, the values of " +
			"AP correlation for the given files truncated to the given number of binary " +
			"fractional digits, in the same order, will be printed to standard output." +
			"If there is more than one value, the vectors will be loaded in memory just " +
			"once and copied across computations.",
			new Parameter[] {
			new Switch("reverse", 'r', "reverse", "Use reversed ranks."),
			new FlaggedOption("type", JSAP.STRING_PARSER,  "double", JSAP.NOT_REQUIRED, 't', "type", "The type of the input files, of the form kind[:kind] where kind is one of int, long, float, double, text"),
			new UnflaggedOption("file0", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The first rank file."),
			new UnflaggedOption("file1", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The second rank file."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String f0 = jsapResult.getString("file0");
		final String f1 = jsapResult.getString("file1");
		final boolean reverse = jsapResult.userSpecified("reverse");
		final Class<?>[] inputType = parseInputTypes(jsapResult);

		int[] digits = jsapResult.getIntArray("digits");
		if (digits.length == 0) digits = new int[] { Integer.MAX_VALUE };

		if (digits.length == 1) System.out.println(INSTANCE.compute(f0, inputType[0], f1, inputType[1], reverse, digits[0]));
		else {
			final double[] v0 = loadAsDoubles(f0, inputType[0], reverse), v1 = loadAsDoubles(f1, inputType[1], reverse);
			for(int d: digits) System.out.println(INSTANCE.compute(Precision.truncate(v0.clone(), d), Precision.truncate(v1.clone(), d)));
		}
	}
}

package it.unimi.dsi.law.util;

/*
 * Copyright (C) 2011-2019 Sebastiano Vigna
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

//RELEASE-STATUS: DIST

import java.io.IOException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.law.stat.CorrelationIndex;
import it.unimi.dsi.law.stat.KendallTau;

/** An {@link Enum} providing different &#x2113; norms. */

public enum Norm {
	/** The {@linkplain #compute(double[]) &#x2113;<sub>1</sub> norm} of a vector is the sum of the absolute
	 * values of its components. We use <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan's
	 * summation algorithm</a> to contain numerical errors. */
	L_1 {
		@Override
		public double compute(final double[] v) {
			double normL1 = 0, c = 0;
			for (int i = v.length; i-- != 0;) {
				final double y = Math.abs(v[i]) - c;
				final double t = normL1 + y;
				c = (t - normL1) - y;
				normL1 = t;
			}
			return normL1;
		}

		@Override
		public double compute(final double[][] bv) {
			double normL1 = 0, c = 0;
			for(final double[] v: bv) {
				for (int i = v.length; i-- != 0;) {
					final double y = Math.abs(v[i]) - c;
					final double t = normL1 + y;
					c = (t - normL1) - y;
					normL1 = t;
				}
			}
			return normL1;
		}

		@Override
		public double compute(final double[] v, final double[] w) {
			if (v.length != w.length) throw new IllegalArgumentException("The two vectors have different sizes: " + v.length + " != " + w.length);

			double normL1 = 0, c = 0;
			for (int i = v.length; i-- != 0;) {
				final double y = Math.abs(v[i] - w[i]) - c;
				final double t = normL1 + y;
				c = (t - normL1) - y;
				normL1 = t;
			}
			return normL1;
		}

		@Override
		public double compute(final double[][] bv, final double[][] bw) {
			if (DoubleBigArrays.length(bv) != DoubleBigArrays.length(bw)) throw new IllegalArgumentException("The two big vectors have different sizes: " + DoubleBigArrays.length(bv) + " != " + DoubleBigArrays.length(bw));

			double normL1 = 0, c = 0;
			for(int s = bv.length; s-- != 0;) {
				final double[] v = bv[s];
				final double[] w = bw[s];
				for (int i = v.length; i-- != 0;) {
					final double y = Math.abs(v[i] - w[i]) - c;
					final double t = normL1 + y;
					c = (t - normL1) - y;
					normL1 = t;
				}
			}
			return normL1;
		}
	},
	/** The {@linkplain #compute(double[]) &#x2113;<sub>2</sub> norm} of a vector is the square root of the sum of the squares
	 * of its components. We use <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan's
	 * summation algorithm</a> to contain numerical errors.*/
	L_2 {
		@Override
		public double compute(final double[] v) {
			double sumOfSquares = 0, c = 0;
			for(int i = v.length ; i-- != 0;) {
				final double y = (v[i] * v[i]) - c;
				final double t = sumOfSquares + y;
				c = (t - sumOfSquares) - y;
				sumOfSquares = t;
			}
			return Math.sqrt(sumOfSquares);
		}

		@Override
		public double compute(final double[][] bv) {
			double sumOfSquares = 0, c = 0;
			for(final double[] v: bv) {
				for(int i = v.length ; i-- != 0;) {
					final double y = (v[i] * v[i]) - c;
					final double t = sumOfSquares + y;
					c = (t - sumOfSquares) - y;
					sumOfSquares = t;
				}
			}
			return Math.sqrt(sumOfSquares);
		}

		@Override
		public double compute(final double[] v, final double[] w) {
			if (v.length != w.length) throw new IllegalArgumentException("The two vectors have different sizes: " + v.length + " != " + w.length);

			double sumOfSquares = 0, c = 0;
			for(int i = v.length; i-- != 0;) {
				final double y = (v[i] - w[i]) * (v[i] - w[i]) - c;
				final double t = sumOfSquares + y;
				c = (t - sumOfSquares) - y;
				sumOfSquares = t;
			}
			return Math.sqrt(sumOfSquares);
		}

		@Override
		public double compute(final double[][] bv, final double[][] bw) {
			if (DoubleBigArrays.length(bv) != DoubleBigArrays.length(bw)) throw new IllegalArgumentException("The two big vectors have different sizes: " + DoubleBigArrays.length(bv) + " != " + DoubleBigArrays.length(bw));

			double sumOfSquares = 0, c = 0;
			for(int s = bv.length; s-- != 0; ) {
				final double[] v = bv[s];
				final double[] w = bw[s];
				for (int i = v.length; i-- != 0;) {
					final double y = (v[i] - w[i]) * (v[i] - w[i]) - c;
					final double t = sumOfSquares + y;
					c = (t - sumOfSquares) - y;
					sumOfSquares = t;
				}
			}
			return Math.sqrt(sumOfSquares);
		}
	},
	/** The {@linkplain #compute(double[]) &#x2113;<sub>&#x221E;</sub> norm} of a vector is the maximum of the absolute
	 * values of its components. */
	L_INFINITY {
		@Override
		public double compute(final double[] v) {
			double norm = 0;
			for (int i = v.length; i-- != 0;) norm = Math.max(norm, Math.abs(v[i]));
			return norm;
		}

		@Override
		public double compute(final double[][] bv) {
			double norm = 0;
			for(final double[] v : bv)
				for (int i = v.length; i-- != 0;) norm = Math.max(norm, Math.abs(v[i]));
			return norm;
		}

		@Override
		public double compute(final double[] v, final double[] w) {
			if (v.length != w.length) throw new IllegalArgumentException("The two vectors have different sizes: " + v.length + " != " + w.length);

			double norm = 0;
			for (int i = v.length; i-- != 0;) norm = Math.max(norm, Math.abs(v[i] - w[i]));
			return norm;
		}

		@Override
		public double compute(final double[][] bv, final double[][] bw) {
			if (DoubleBigArrays.length(bv) != DoubleBigArrays.length(bw)) throw new IllegalArgumentException("The two big vectors have different sizes: " + DoubleBigArrays.length(bv) + " != " + DoubleBigArrays.length(bw));

			double norm = 0;
			for(int s = bv.length; s-- != 0; ) {
				final double[] v = bv[s];
				final double[] w = bw[s];
				for (int i = v.length; i-- != 0;) norm = Math.max(norm, Math.abs(v[i] - w[i]));
			}
			return norm;
		}
	};

	/** Computes the norm of a vector.
	 *
	 * @param v a vector.
	 * @return the norm of <code>v</code>.
	 */
	public abstract double compute(final double[] v);

	/** Computes the norm of the difference of two vectors.
	 *
	 * @param v the first vector.
	 * @param w the second vector.
	 * @return the norm of <code>v</code>&nbsp;&minus;&nbsp;<code>w</code>.
	 */
	public abstract double compute(final double[] v, final double[] w);

	/** Computes the norm of a big vector.
	 *
	 * @param v a big vector.
	 * @return the norm of <code>v</code>.
	 */
	public abstract double compute(final double[][] v);

	/** Computes the norm of the difference of two big vectors.
	 *
	 * @param v the first big vector.
	 * @param w the second big vector.
	 * @return the norm of <code>v</code>&nbsp;&minus;&nbsp;<code>w</code>.
	 */
	public abstract double compute(final double[][] v, final double[][] w);

	/** Computes the norm of a big vector.
	 *
	 * @param v a big vector.
	 * @return the norm of <code>v</code>.
	 */

	/** Normalizes a vector to a given norm value.
	 *
	 * @param v the vector to be normalized.
	 * @param norm the new norm value (nonnegative).
	 * @return <code>v</code>.
	 */
	public double[] normalize(final double[] v, final double norm) {
		if (norm < 0) throw new IllegalArgumentException("Negative norm: " + norm);
		final double c = norm / compute(v);
		for (int i = v.length; i-- != 0;) v[i] *= c;
		return v;
	}


	public static void main(final String[] arg) throws NumberFormatException, IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(KendallTau.class.getName(),
			"Computes the L1- or L2-norm of a vector of doubles (or of the difference between two vectors of doubles) contained in a (two) given file. " +
			"The file(s) must contain the same number of doubles, written " +
			"in Java binary format, or in some other format if -t is specified.",
			new Parameter[] {
			new FlaggedOption("type", JSAP.STRING_PARSER,  "double", JSAP.NOT_REQUIRED, 't', "type", "The type of the input files, of the form type[:type] where type is one of int, long, float, double, text"),
			new FlaggedOption("norm", JSAP.INTEGER_PARSER,  "2", JSAP.NOT_REQUIRED, 'n', "norm", "The type of norm (1=L1, 2=L2)."),
			new UnflaggedOption("file0", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The first rank file."),
			new UnflaggedOption("file1", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The second rank file."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final int normNumber = jsapResult.getInt("norm");
		if (normNumber != 1 && normNumber != 2) throw new IllegalArgumentException("Type must be 1 or 2");
		final Norm norm = normNumber == 1? L_1 : L_2;

		final Class<?>[] inputType = CorrelationIndex.parseInputTypes(jsapResult);

		final String f0 = jsapResult.getString("file0");
		final String f1 = jsapResult.getString("file1");

		final double[] v0 = CorrelationIndex.loadAsDoubles(f0, inputType[0], false);
		final double[] v1 = f1 == null? null : CorrelationIndex.loadAsDoubles(f1, inputType[1], false);

		final double result = v1 == null? norm.compute(v0) : norm.compute(v0, v1);
		System.out.println(result);

	}

}

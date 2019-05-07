package it.unimi.dsi.law.stat;

import java.io.IOException;

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
 * Copyright (C) 2013-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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
import it.unimi.dsi.fastutil.ints.AbstractInt2DoubleFunction;
import it.unimi.dsi.fastutil.ints.Int2DoubleFunction;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.law.util.ExchangeWeigher;
import it.unimi.dsi.law.util.Precision;

// RELEASE-STATUS: DIST

/** Computes the weighted &tau; between two score vectors. More precisely, this class computes the formula given by
 * Sebastiano Vigna in &ldquo;<a href="http://vigna.di.unimi.it/papers.php#VigWCIRT">A weighted correlation index for rankings
 * with ties</a>&rdquo;, <i>Proc&#46; of the 24th International World&ndash;Wide Web
 * Conference</i>, pages 1166&minus;1176, 2015, ACM Press, using the algorithm therein described (see details below).
 *
 * <P>Given two scores vectors for a list of items,
 * this class provides a {@linkplain #compute(double[], double[]) method to compute efficiently the weighted &tau;}
 * using an {@link ExchangeWeigher}.
 *
 * <p>Instances of this class are immutable. At creation time you can specify a
 * <em>weigher</em> that turns indices into weights, and
 * whether to combine weights additively or multiplicatively.
 * Ready-made weighers include {@link #HYPERBOLIC_WEIGHER}, which is the weigher of choice. Alternatives include
 * {@link #LOGARITHMIC_WEIGHER} and {@link #QUADRATIC_WEIGHER}.
 *
 * Additional methods inherited from {@link CorrelationIndex} make it possible to
 * compute directly the weighted &tau; bewteen two files, to bound the number of significant digits, or
 * to reverse the standard association between scores and ranks (by default,
 * a larger score corresponds to a higher rank, i.e., to a smaller rank index; the largest score gets
 * rank 0).
 *
 * <p>The <em>weighted</em> &tau; is defined as follows: consider a <em>rank</em> function &rho; (returning
 * natural numbers or &infin;) that provides a <em>ground truth</em>&mdash;it tells us which elements are more or less important. Consider
 * also a weight function <var>w</var>(&minus;, &minus;) associating with each pair of ranks a nonnegative real number. We define
 * the <em>rank-weighted &tau;</em> by
 * <div align=center>
 * &#x3008;<b><var>r</var></b>, <b><var>s</var></b>&#x3009;<sub>&rho;,<var>w</var></sub> = <big>&Sigma;</big><sub><var>i</var>,&nbsp;<var>j</var></sub>
 * sgn(<var>r</var><sub><var>i</var></sub> &minus; <var>r</var><sub><var>j</var></sub>)
 * sgn(<var>s</var><sub><var>i</var></sub> &minus; <var>s</var><sub><var>j</var></sub>) <var>w</var>(&rho;(<var>i</var>), &rho;(<var>j</var>))
 * </div>
 * <div align=center>
 * &#x2016;<b><var>r</var></b>&#x2016;<sub>&rho;,<var>w</var></sub> =  &#x3008;<b><var>r</var></b>, <b><var>r</var></b>&#x3009;<sub>&rho;,<var>w</var></sub><sup>1/2</sup>
 * </div>
 * <div align=center>
 * &tau;<sub>&rho;,<var>w</var></sub>(<b><var>r</var></b>, <b><var>s</var></b>) = &#x3008;<b><var>r</var></b>, <b><var>s</var></b>&#x3009;<sub>&rho;,<var>w</var></sub> / (&#x2016;<b><var>r</var></b>&#x2016;<sub>&rho;,<var>w</var></sub> &#x2016;<b><var>s</var></b>&#x2016;<sub>&rho;,<var>w</var></sub>).
 * </div>
 *
 * <p>The weight function can be specified by giving a weigher <var>f</var> (e.g., {@link #HYPERBOLIC_WEIGHER}) and a combination
 * strategy, which can be additive or multiplicative.
 * The weight of the exchange between <var>i</var> and <var>j</var>
 * is then <var>f</var>(<var>i</var>) &#9679; <var>f</var>(<var>j</var>), where &#9679; is the chosen combinator.
 *
 * <p>Now, consider the rank function &rho;<sub><b><var>r</var></b>, <b><var>s</var></b></sub> induced
 * by the lexicographical order by <b><var>r</var></b> and <b><var>s</var></b></sub>. We define
 * <div align=center>
 * &tau;<sub><var>w</var></sub> = (&tau;<sub>&rho;<sub><b><var>r</var></b>, <b><var>s</var></b></sub>, <var>w</var></sub> + &tau;<sub>&rho;<sub><b><var>s</var></b>, <b><var>r</var></b></sub>, <var>w</var></sub>) / 2.
 * </div>
 *
 * <p>In particular, the (additive) <em>hyperbolic &tau;</em> is defined by the weight function <var>h</var>(<var>i</var>) = 1 / (<var>i</var> + 1) combined additively:
 * <div align=center>
 * &tau;<sub>h</sub> = (&tau;<sub>&rho;<sub><b><var>r</var></b>, <b><var>s</var></b></sub>, <var>h</var></sub> + &tau;<sub>&rho;<sub><b><var>s</var></b>, <b><var>r</var></b></sub>, <var>h</var></sub>) / 2.
 * </div>
 *
 * <p>The methods inherited from {@link CorrelationIndex} compute the formula above using the provided weigher
 * and combination method. A ready-made instance {@link #HYPERBOLIC} can be used to compute the additive hyperbolic &tau;. An
 * <i>ad hoc</i> {@linkplain #compute(double[], double[], int[]) method} can instead compute &tau;<sub>&rho;,<var>w</var></sub>.
 *
 * <p>A main method is provided for command-line usage.
 */

public class WeightedTau extends CorrelationIndex {
	private final static Logger LOGGER = LoggerFactory.getLogger(WeightedTau.class);

	public static abstract class AbstractWeigher extends AbstractInt2DoubleFunction {
		private static final long serialVersionUID = 1L;
		@Override
		public boolean containsKey(final int x) {
			return x >= 0;
		}
		@Override
		public int size() {
			return -1;
		}
	}

	private static final class HyperbolicWeigher extends AbstractWeigher {
		private static final long serialVersionUID = 1L;
		@Override
		public double get(final int x) {
			return 1. / (x + 1);
		}
	}

	/** A hyperbolic weigher (the default one). Rank <var>x</var> has weight 1 / (<var>x</var> + 1). */
	public static final Int2DoubleFunction HYPERBOLIC_WEIGHER = new HyperbolicWeigher();

	private static final class QuadraticWeigher extends AbstractWeigher {
		private static final long serialVersionUID = 1L;
		@Override
		public double get(final int x) {
			double xPlus1 = x + 1.;
			return 1. / (xPlus1 * xPlus1);
		}
	}

	/** A quadratic weigher. Rank <var>x</var> has weight 1 / (<var>x</var> + 1)<sup>2</sup>. */
	public static final Int2DoubleFunction QUADRATIC_WEIGHER = new QuadraticWeigher();

	private static final class LogarithmicWeigher extends AbstractWeigher {
		private static final long serialVersionUID = 1L;
		@Override
		public double get(final int x) {
			return 1. / Math.log(x + Math.E);
		}
	}

	/** A logarithmic weigher. Rank <var>x</var> has weight 1 / ln(<var>x</var> + <var>e</var>). */
	public static final Int2DoubleFunction LOGARITHMIC_WEIGHER = new LogarithmicWeigher();

	private static final class ZeroWeigher extends AbstractWeigher {
		private static final long serialVersionUID = 1L;
		@Override
		public double get(final int x) {
			return 0;
		}
	}

	/** A constant zero weigher. */
	public static final Int2DoubleFunction ZERO_WEIGHER = new ZeroWeigher();

	/** A singleton instance of the symmetric hyperbolic additive &tau;.*/
	public final static WeightedTau HYPERBOLIC = new WeightedTau();

	/** The weigher. */
	private final Int2DoubleFunction weigher;
	/** Whether to multiply weights, rather than adding them. */
	private final boolean multiplicative;

	/** Create an additive hyperbolic &tau;.
	 */
	public WeightedTau() {
		this(HYPERBOLIC_WEIGHER);
	}

	/** Create an additive weighted &tau; using the specified weigher.
	 *
	 * @param weigher a weigher.
	 */
	public WeightedTau(final Int2DoubleFunction weigher) {
		this(weigher, false);
	}

	/** Create an additive or multiplicative weighted &tau; using the specified weigher and combination strategy.
	 *
	 * @param weigher a weigher.
	 * @param multiplicative if true, weights are combined multiplicatively, rather than additively.
	 */
	public WeightedTau(final Int2DoubleFunction weigher, final boolean multiplicative) {
		this.weigher = weigher;
		this.multiplicative = multiplicative;
	}

	/** Computes the symmetrized weighted &tau; between two score vectors.
	 *
	 * @param v0 the first score vector.
	 * @param v1 the second score vector.
	 * @return the symmetric weighted &tau;.
	 */
	public double compute(final double v0[], final double v1[]) {
		// Ensure interval [-1..1] (small deviations might appear because of numerical errors).
		return Math.min(1, Math.max(-1, (compute(v0, v1, null) + compute(v1, v0, null)) / 2));
	}

	/** Computes the weighted &tau; between two score vectors, given a reference rank.
	 *
	 * <p>Note that this method must be called with some care. More precisely, the two
	 * arguments should be built on-the-fly in the method call, and not stored in variables,
	 * as the first argument array will be {@code null}'d during the execution of this method
	 * to free some memory: if the array is referenced elsewhere the garbage collector will not
	 * be able to collect it.
	 *
	 * @param v0 the first score vector.
	 * @param v1 the second score vector.
	 * @param rank the &ldquo;ground truth&rdquo; ranking used to weight exchanges, or {@code null} to use the
	 * ranking induced lexicographically by {@code v1} and {@code v0} as ground truth.
	 * @return the weighted &tau;.
	 */
	public double compute(final double v0[], double v1[], int[] rank) {
		if (v0.length != v1.length) throw new IllegalArgumentException("Array lengths differ: " + v0.length + ", " + v1.length);
		final int length = v0.length;
		if (length == 0) throw new IllegalArgumentException("The weighted τ is undefined on empty rankings");
		if (rank != null && rank.length != length) throw new IllegalArgumentException("The score array length (" + length + ") and the rank array length (" + rank.length + ") do not match");

		final int[] perm = Util.identity(length);

		// First of all we sort perm stably by the second score vector, and then by the first in case of a tie.
		DoubleArrays.radixSortIndirect(perm, v1, v0, true);

		if (rank == null) {
			// To generate a rank array, we must first reverse the permutation (to get higher ranks first) and then invert it.
			rank = perm.clone();
			IntArrays.reverse(rank);
			Util.invertPermutationInPlace(rank);
		}

		// Next, we compute weight of joint ties.
		int i, first = 0;
		double t = 0;
		double w = weigher.get(rank[perm[first]]);
		double s = w;
		double sq = w * w;

		for(i = 1; i < length; i++) {
			if (v0[perm[first]] != v0[perm[i]] || v1[perm[first]] != v1[perm[i]]) {
				t += multiplicative ? (s * s - sq) / 2 : s * (i - first - 1);
				first = i;
				s = sq = 0;
			}
			w = weigher.get(rank[perm[i]]);
			s += w;
			sq += w * w;
		}

		t += multiplicative ? (s * s - sq) / 2 : s * (i - first - 1); // Last block

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Weight of joint ties: " + t);

		// Now we compute the weight of ties in the second score vector.
		first = 0;
		double v = 0;
		w = weigher.get(rank[perm[first]]);
		s = w;
		sq = w * w;
		for(i = 1; i < length; i++) {
			if (v1[perm[first]] != v1[perm[i]]) {
				v += multiplicative ? (s * s - sq) / 2 : s * (i - first - 1);
				first = i;
				s = sq = 0;
			}
			w = weigher.get(rank[perm[i]]);
			s += w;
			sq += w * w;
		}

		v += multiplicative ? (s * s - sq) / 2 : s * (i - first - 1); // Last block

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Weight of ties in the second score vector: " + v);

		// We null v0 so that the garbage collector can reclaim it.
		v1 = null;

		// Now we use an exchange weigher to order stably by the first score vector and weigh the exchanges (i.e., discordances).
		final double exchanges = new ExchangeWeigher(weigher, perm, v0, rank, multiplicative, new int[length]).weigh();
		if (LOGGER.isDebugEnabled()) LOGGER.debug("Weight of exchanges: " + exchanges);

		// Now we compute the weight of ties in the first score vector.
		first = 0;
		double u = 0;
		w = weigher.get(rank[perm[first]]);
		s = w;
		sq = w * w;
		for(i = 1; i < length; i++) {
			if (v0[perm[first]] != v0[perm[i]]) {
				u += multiplicative ? (s * s - sq) / 2 : s * (i - first - 1);
				first = i;
				s = sq = 0;
			}
			w = weigher.get(rank[perm[i]]);
			s += w;
			sq += w * w;
		}

		u += multiplicative ? (s * s - sq) / 2 : s * (i - first - 1); // Last block

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Weight of ties in the first score vector: " + u);

		s = sq = 0;
		for(i = 0; i < length; i++) {
			w = weigher.get(rank[perm[i]]);
			s += w;
			sq += w * w;
		}

		final double tot = multiplicative ? (s * s - sq) / 2 : s * (length - 1);

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Total weight: " + tot);
		// Special case for all ties in both ranks
		if (tot == u && tot == v) return 1;

		// System.out.println(tot + " " + u + " " + v + " " + exchanges + " -> " + Math.min(1, Math.max(-1, (tot - v - u + t - 2 * exchanges) / Math.sqrt((tot - u) * (tot - v)))));

		return Math.min(1, Math.max(-1, (tot - v - u + t - 2 * exchanges) / Math.sqrt((tot - u) * (tot - v))));
	}


	public static void main(String[] arg) throws NumberFormatException, IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(WeightedTau.class.getName(),
			"Computes a weighted correlation index between two given score files. " +
			"By default, the index is a symmetric additive hyperbolic τ, but you can set a different choice " +
			"using the available options. Note that scores need not to be distinct (i.e., you can have an arbitrary number of ties)." +
			"\n" +
			"By default, the two files must contain the same number of doubles, written " +
			"in Java binary (DataOutput) format. The option -t makes it possible to specify a different " +
			"type (possibly for each input file)." +
			"\n" +
			"If one or more truncations are specified with the option -T, the values " +
			"of specified weighted correlation index for the given files truncated to the given number of binary " +
			"fractional digits, in the same order, will be printed to standard output." +
			"If there is more than one value, the vectors will be loaded in memory just " +
			"once and copied across computations.",
			new Parameter[] {
			new Switch("reverse", 'r', "reverse", "Use reverse ranks (that is, rank decreases as score increases)."),
			new Switch("logarithmic", 'l', "logarithmic", "Use a logarithmic (instead of hyperbolic) weight."),
			new Switch("quadratic", 'q', "quadratic", "Use a quadratic (instead of hyperbolic) weight."),
			new Switch("multiplicative", 'm', "multiplicative", "Use a multiplicative (instead of additive) combination of weights."),
			new FlaggedOption("type", JSAP.STRING_PARSER,  "double", JSAP.NOT_REQUIRED, 't', "type", "The type of the input files, of the form type[:type] where type is one of int, long, float, double, text"),
			new FlaggedOption("digits", JSAP.INTEGER_PARSER,  JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "truncate", "Truncate inputs to the given number of binary fractional digits.").setAllowMultipleDeclarations(true),
			new UnflaggedOption("file0", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The first score file."),
			new UnflaggedOption("file1", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The second score file."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String f0 = jsapResult.getString("file0");
		final String f1 = jsapResult.getString("file1");
		final boolean reverse = jsapResult.userSpecified("reverse");

		final boolean logarithmic = jsapResult.userSpecified("logarithmic");
		final boolean quadratic = jsapResult.userSpecified("quadratic");
		final boolean multiplicative = jsapResult.userSpecified("multiplicative");
		if (logarithmic && quadratic) throw new IllegalArgumentException("You cannot specify logarithmic and quadratic weighting at the same time");

		final Class<?>[] inputType = parseInputTypes(jsapResult);

		int[] digits = jsapResult.getIntArray("digits");
		if (digits.length == 0) digits = new int[] { Integer.MAX_VALUE };

		final WeightedTau weightedTau = new WeightedTau(logarithmic
			? LOGARITHMIC_WEIGHER
			: quadratic
				? QUADRATIC_WEIGHER
				: HYPERBOLIC_WEIGHER, multiplicative);

		if (digits.length == 1) System.out.println(weightedTau.compute(f0, inputType[0], f1, inputType[1], reverse, digits[0]));
		else {
			final double[] v0 = loadAsDoubles(f0, inputType[0], reverse), v1 = loadAsDoubles(f1, inputType[1], reverse);
			for(int d: digits) System.out.println(weightedTau.compute(Precision.truncate(v0.clone(), d), Precision.truncate(v1.clone(), d)));
		}
	}
}

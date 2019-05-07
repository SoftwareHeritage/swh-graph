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

import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.law.util.ExchangeCounter;
import it.unimi.dsi.law.util.Precision;

// RELEASE-STATUS: DIST

/** Computes Kendall's &tau; between two score vectors. More precisely, the this class computes the formula given by
 * Kendall in &ldquo;The treatment of ties in ranking problems&rdquo;, <i>Biometrika</i> 33:239&minus;251, 1945.
 *
 * <p>Note that in the literature the 1945 definition is often called &tau;<sub><i>b</i></sub>, and &tau; is reserved for
 * the original coefficient (&ldquo;A new measure of rank correlation&rdquo;, <i>Biometrika</i> 30:81&minus;93, 1938). But
 * this distinction is pointless, as the 1938 paper defines &tau; only for rankings with no ties, and the generalisation in the
 * 1945 paper reduces exactly to the original definition if there are no ties.
 *
 * <P>Given two scores vectors for a list of items,
 * this class provides a {@linkplain #compute(double[], double[]) method to compute efficiently Kendall's &tau;}
 * using an {@link ExchangeCounter}.
 *
 * <p>This class is a singleton: methods must be invoked on {@link #INSTANCE}.
 * Additional methods inherited from {@link CorrelationIndex} make it possible to
 * compute directly the score bewteen two files, or to bound the number of significant digits.
 *
 * <p>More precisely, given <var>r</var><sub><var>i</var></sub> and <var>s</var><sub><var>i</var></sub>
 * (<var>i</var> = 0, 1,&nbsp;&hellip;, <var>n</var>&nbsp;&minus;&nbsp;1), we say that a pair (<var>i</var>, <var>j</var>), <var>i</var>&lt;<var>j</var>, is
 * <ul>
 * <li><em>concordant</em> iff <var>r</var><sub><var>i</var></sub> &minus; <var>r</var><sub><var>j</var></sub> and
 * <var>s</var><sub><var>i</var></sub> &minus; <var>s</var><sub><var>j</var></sub> are both non-zero and
 *   have the same sign;
 * <li><em>discordant</em> iff <var>r</var><sub><var>i</var></sub> &minus; <var>r</var><sub><var>j</var></sub> and
 * <var>s</var><sub><var>i</var></sub> &minus; <var>s</var><sub><var>j</var></sub> are both non-zero and
 *   have opposite signs;
 * <li> an <em><var>r</var>-tie</em> iff <var>r</var><sub><var>i</var></sub> &minus; <var>r</var><sub><var>j</var></sub> = 0;
 * <li> an <em><var>s</var>-tie</em> iff <var>s</var><sub><var>i</var></sub> &minus; <var>s</var><sub><var>j</var></sub> = 0;
 * <li> a <em>joint tie</em> iff <var>r</var><sub><var>i</var></sub> &minus; <var>r</var><sub><var>j</var></sub> = 0
 * and <var>s</var><sub><var>i</var></sub> &minus; <var>s</var><sub><var>j</var></sub> = 0.
 * </ul>
 *
 * <P>Let <var>C</var>, <var>D</var>, <var>T<sub>r</sub></var>, <var>T<sub>s</sub></var>, <var>J</var>
 * be the number of concordant pairs, discordant pairs,
 * <var>r</var>-ties, <var>s</var>-ties and joint ties, respectively, and <var>N</var> = <var>n</var>(<var>n</var> &minus; 1)/2. Of course
 * <var>C</var>+<var>D</var>+<var>T<sub>r</sub></var>+<var>T<sub>s</sub></var> &minus; <var>J</var> = <var>N</var>.
 * Kendall's &tau; is now
 * <blockquote>
 * &tau; = (<var>C</var> &minus; <var>D</var>) / [(<var>N</var> &minus; <var>T<sub>r</sub></var>)(<var>N</var> &minus; <var>T<sub>s</sub></var>)]<sup>1/2</sup>
 * </blockquote>
 *
 * <p>A main method is provided for command-line usage.
 */

public class KendallTau extends CorrelationIndex {
	private static final Logger LOGGER = LoggerFactory.getLogger(KendallTau.class);

	private KendallTau() {}

	/** The singleton instance of this class. */
	public static final KendallTau INSTANCE = new KendallTau();

	/** Computes Kendall's &tau; between two score vectors.
	 *
	 * <p>Note that this method must be called with some care. More precisely, the two
	 * arguments should be built on-the-fly in the method call, and not stored in variables,
	 * as the first argument array will be {@code null}'d during the execution of this method
	 * to free some memory: if the array is referenced elsewhere the garbage collector will not
	 * be able to collect it.
	 *
	 * @param v0 the first score vector.
	 * @param v1 the second score vector.
	 * @return Kendall's &tau;.
	 */
	public double compute(double v0[], final double v1[]) {
		if (v0.length != v1.length) throw new IllegalArgumentException("Array lengths differ: " + v0.length + ", " + v1.length);
		final int length = v0.length;
		if (length == 0) throw new IllegalArgumentException("Kendall's τ is undefined on empty rankings");

		final int[] perm = Util.identity(length);

		// First of all we sort perm stably by the first rank vector (higher ranks come first!), and then by the second in case of a tie.
		DoubleArrays.radixSortIndirect(perm, v0, v1, true);

		// Next, we compute the number of joint ties.
		int i, first = 0;
		long t = 0;
		for(i = 1; i < length; i++) {
			if (v0[perm[first]] != v0[perm[i]] || v1[perm[first]] != v1[perm[i]]) {
				t += ((i - first) * (i - first - 1L)) / 2;
				first = i;
			}
		}

		t += ((i - first) * (i - first - 1L)) / 2; // Last block

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Joint ties: " + t);

		// Now we compute the number of ties.
		first = 0;
		long u = 0;
		for(i = 1; i < length; i++) {
			if (v0[perm[first]] != v0[perm[i]]) {
				u += ((i - first) * (i - first - 1L)) / 2;
				first = i;
			}
		}

		u += ((i - first) * (i - first - 1L)) / 2; // Last block

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Ties after first ordering: " + u);

		// We null v0 so that the garbage collector can reclaim it.
		v0 = null;

		// Now we use an exchange counter to order stably by the second rank and count the number of exchanges (i.e., discordances).
		final long exchanges = new ExchangeCounter(perm, v1).count();

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Exchanges: " + exchanges);

		// Now we compute the number of ties.
		first = 0;
		long v = 0;
		for(i = 1; i < length; i++) {
			if (v1[perm[first]] != v1[perm[i]]) {
				v += ((i - first) * (i - first - 1L)) / 2;
				first = i;
			}
		}

		v += ((i - first) * (i - first - 1L)) / 2; // Last block

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Ties after second ordering: " + v);

		final long tot = (length * (length - 1L)) / 2;

		if (LOGGER.isDebugEnabled()) LOGGER.debug("Combinations of order two: " + tot);

		// Special case for all ties in both ranks
		if (tot == u && tot == v) return 1;
		// Ensure interval [-1..1] (small deviations might appear because of numerical errors).
		return Math.min(1, Math.max(-1, ((tot - (v + u - t)) - 2.0 * exchanges) / (Math.sqrt((double) (tot - u) * (double) (tot - v)))));
	}

	public static void main(String[] arg) throws NumberFormatException, IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(KendallTau.class.getName(),
			"Computes Kendall's τ between the score vectors contained in two given files. " +
			"The two files must contain the same number of doubles, written " +
			"in Java binary format. The option -t makes it possible to specify a different " +
			"type (possibly for each input file)." +
			"\n" +
			"If one or more truncations are specified with the option -T, the values of " +
			"Kendall's τ for the given files truncated to the given number of binary " +
			"fractional digits, in the same order, will be printed to standard output." +
			"If there is more than one value, the vectors will be loaded in memory just " +
			"once and copied across computations.",
			new Parameter[] {
			new FlaggedOption("type", JSAP.STRING_PARSER,  "double", JSAP.NOT_REQUIRED, 't', "type", "The type of the input files, of the form type[:type] where type is one of int, long, float, double, text"),
			new FlaggedOption("digits", JSAP.INTEGER_PARSER,  JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "truncate", "Truncate inputs to the given number of binary fractional digits.").setAllowMultipleDeclarations(true),
			new UnflaggedOption("file0", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The first rank file."),
			new UnflaggedOption("file1", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The second rank file."),
		}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String f0 = jsapResult.getString("file0");
		final String f1 = jsapResult.getString("file1");
		final Class<?>[] inputType = parseInputTypes(jsapResult);

		int[] digits = jsapResult.getIntArray("digits");
		if (digits.length == 0) digits = new int[] { Integer.MAX_VALUE };

		if (digits.length == 1) System.out.println(INSTANCE.compute(f0, inputType[0], f1, inputType[1], digits[0]));
		else {
			final double[] v0 = loadAsDoubles(f0, inputType[0], false), v1 = loadAsDoubles(f1, inputType[1], false);
			for(int d: digits) System.out.println(INSTANCE.compute(Precision.truncate(v0.clone(), d), Precision.truncate(v1.clone(), d)));
		}
	}
}

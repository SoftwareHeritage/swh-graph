package it.unimi.dsi.law.rank;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

import org.slf4j.Logger;

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

import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.util.Properties;
import it.unimi.dsi.webgraph.ImmutableGraph;


// RELEASE-STATUS: DIST

/** An abstract class defining methods and attributes supporting PageRank computations. Provides
 * settable damping factor, preference vector and starting vector.
 *
 * <h2>Formulae and preferences</h2>
 *
 * <p>There are two main formulae for PageRank in the literature. The first one, which we shall
 * call <em>weakly preferential</em>, patches all dangling nodes by adding a uniform transition towards
 * all other nodes. The second one, which we shall call <em>strongly preferential</em>, patches all
 * dangling nodes adding transitions weighted following the preference vector <var><b>v</b></var>.
 * We can consider the two formulae together, letting <var><b>u</b></var> be a vector that is uniform
 * in the weak case and coincides with <var><b>v</b></var> in the strong case.
 *
 * <P>If we denote with <var>P</var> the normalised adjacency matrix of the graph, with <var><b>d</b></var>
 * the characteristic vector of dangling nodes, and with &alpha; the damping factor, the generic
 * equation is
 * <div style="text-align: center">
 * <var><b>x</b></var> = <var><b>x</b></var> (&alpha; <var>P</var> + &alpha; <var><b>d</b><sup><i>T</i></sup></var><var><b>u</b></var> +  (1 &minus; &alpha;) <b>1</b><sup><i>T</i></sup> <var><b>v</b></var>),
 * </div>
 * which, distributing over the sum, makes it possible to express PageRank as
 * <div style="text-align: center">
 * (1 &minus; &alpha;) <var><b>v</b></var><big>(</big>  <var>P</var> + <var><b>d</b><sup><i>T</i></sup></var><var><b>u</b></var> <big>)</big><sup>-1</sup>,
 * </div>
 * or even
 * <div style="text-align: center">
 * (1 &minus; &alpha;) <var><b>v</b></var> <big>&Sigma;</big><sub><var>k</var> &ge; 0</sub> &alpha;<sup><var>k</var></sup> <big>(</big> <var>P</var> + <var><b>d</b><sup><i>T</i></sup></var><var><b>u</b></var> <big>)</big><sup><var>k</var></sup>.
 * </div>
 *
 * <p>By default, weakly preferential PageRank is computed; strongly preferential
 * PageRank computation is enforced by setting {@link #stronglyPreferential} to true.
 *
 * @see SpectralRanking
 */

public abstract class PageRank extends SpectralRanking {
	/** The default damping factor. */
	public final static double DEFAULT_ALPHA = 0.85;

	/** The damping factor. In the random surfer interpretation, this is the probability that the
	 * surfer will follow a link in the current page. */
	public double alpha = DEFAULT_ALPHA;
	/** The preference vector to be used (or {@code null} if the uniform preference vector should be used). */
	public DoubleList preference;
	/** The vector used used to patch null rows of the adjacency matrix (<b><var>u</var></b> in the general formula).
	 *  It coincides with the preference vector if {@link #stronglyPreferential} is true. If {@code null},
	 *  the uniform distribution will be used. */
	public DoubleList danglingNodeDistribution;
	/** If not {@code null}, the set of buckets of {@link #graph}. */
	public BitSet buckets;
	/** Decides whether we use the strongly or weakly (the default) preferential algorithm. */
	public boolean stronglyPreferential;

	/** Creates a new instance.
	 *
	 * @param g the graph.
	 * @param logger a logger.
	 */
	public PageRank(final ImmutableGraph g, final Logger logger) {
		super(g, logger);
	}

	/** Returns a {@link Properties} object that contains all the parameters used by the computation.
	 *
	 * @param graphBasename the basename of the graph.
	 * @param preferenceFilename the filename of preference vector. It can be {@code null}.
	 * @param danglingFilename the filename of dangling-node distribution. It can be {@code null}.
	 * @return a properties object that represent all the parameters used to calculate the rank.
	 */
	public Properties buildProperties(String graphBasename, String preferenceFilename, String danglingFilename) {
		final Properties prop = super.buildProperties(graphBasename);
		prop.setProperty("alpha", Double.toString(alpha));
		prop.setProperty("norm", normDelta());
		prop.setProperty("stronglypreferential", stronglyPreferential);
		if (preferenceFilename != null) prop.setProperty("preferencefilename", preferenceFilename);
		if (danglingFilename != null) prop.setProperty("danglingfilename", danglingFilename);
		return prop;
	}

	/** Basic initialization: we log the damping factor, check that the preference vector is correctly sized and stochastic,
	 * fill {@link #rank} with the preference vector and set the dangling-node distribution
	 * depending on the value of {@link #stronglyPreferential}.
	 */
	@Override
	public void init() throws IOException {
		super.init();
		logger.info("Damping factor: " + alpha);

		// Check the preference vector
		if (preference != null) {
			if (preference.size() != n) throw new IllegalArgumentException("The preference vector size (" + preference.size() + ") is different from graph dimension (" + n + ").");
			if (! isStochastic(preference)) throw new IllegalArgumentException("The preference vector is not a stochastic vector. ");
			logger.info("Using a specified preference vector");
		}
		else logger.info("Using the uniform preference vector");

		if (preference != null) preference.toArray(rank);
		else Arrays.fill(rank, 1. / n);

		// Initializes the preferentialAdjustment vector
		if (stronglyPreferential) {
			if (preference == null) throw new IllegalArgumentException("The strongly preferential flag is true but the preference vector has not been set.");
			danglingNodeDistribution = preference;
		}
		else danglingNodeDistribution = null;
		logger.info("Computing " + (stronglyPreferential ? "strongly" : "weakly") + " preferential PageRank");
	}
}

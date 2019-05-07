package it.unimi.dsi.law.big.rank;

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

import java.io.IOException;

import org.slf4j.Logger;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.util.Properties;


// RELEASE-STATUS: DIST

/** A big version of {@link it.unimi.dsi.law.rank.PageRank}.
 *
 * @see it.unimi.dsi.law.rank.PageRank
 * @see SpectralRanking
 */

public abstract class PageRank extends SpectralRanking {
	/** The default damping factor. */
	public final static double DEFAULT_ALPHA = 0.85;

	/** The damping factor. In the random surfer interpretation, this is the probability that the
	 * surfer will follow a link in the current page. */
	public double alpha = DEFAULT_ALPHA;
	/** The preference vector to be used (or {@code null} if the uniform preference vector should be used). */
	public DoubleBigList preference;
	/** The vector used used to patch null rows of the adjacency matrix (<b><var>u</var></b> in the general formula).
	 *  It coincides with the preference vector if {@link #stronglyPreferential} is true. If {@code null},
	 *  the uniform distribution will be used. */
	public DoubleBigList danglingNodeDistribution;
	/** If not {@code null}, the set of buckets of {@link #graph}. */
	public LongArrayBitVector buckets;
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
	public Properties buildProperties(final String graphBasename, final String preferenceFilename, final String danglingFilename) {
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
			if (preference.size64() != n) throw new IllegalArgumentException("The preference vector size (" + preference.size64() + ") is different from graph dimension (" + n + ").");
			if (! isStochastic(preference)) throw new IllegalArgumentException("The preference vector is not a stochastic vector. ");
			logger.info("Using a specified preference vector");
		}
		else logger.info("Using the uniform preference vector");

		if (preference != null) {
			final DoubleIterator iterator = preference.iterator();
			for(int s = 0; s < rank.length; s++) {
				final double[] t = rank[s];
				final int l = t.length;
				for(int d = 0; d < l; d++) t[d] = iterator.nextDouble();
			}
		}
		else DoubleBigArrays.fill(rank, 1.0/n);

		// Initializes the preferentialAdjustment vector
		if (stronglyPreferential) {
			if (preference == null) throw new IllegalArgumentException("The strongly preferential flag is true but the preference vector has not been set.");
			danglingNodeDistribution = preference;
		}
		else danglingNodeDistribution = null;
		logger.info("Computing " + (stronglyPreferential ? "strongly" : "weakly") + " preferential PageRank");
	}
}

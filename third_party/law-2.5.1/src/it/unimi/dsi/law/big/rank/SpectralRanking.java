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
import it.unimi.dsi.fastutil.bytes.ByteBigArrays;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrays;
import it.unimi.dsi.fastutil.doubles.DoubleBigList;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.util.Properties;


// RELEASE-STATUS: DIST

/** A big version of {@link it.unimi.dsi.law.rank.SpectralRanking}.
 * @see it.unimi.dsi.law.rank.SpectralRanking
 * @author Sebastiano Vigna
 */

public abstract class SpectralRanking {
	/** Default threshold (note that this value is used as a default by main methods). */
	public final static double DEFAULT_THRESHOLD = 1E-6;
	/** Default maximum number of iterations (note that this value is used as a default by main methods). */
	public final static int DEFAULT_MAX_ITER = Integer.MAX_VALUE;
	/** The default norm ({@link Norm#L_INFINITY}). */
	public final static Norm DEFAULT_NORM = Norm.L_INFINITY;
	/** The admitted tolerance in the {@linkplain #isStochastic(DoubleList) verification that a vector is a stochastic one}.
	 * A stochastic vector is nonnegative and has &#x2113;<sub>1</sub> norm equal to 1 &plusmn; {@link #STOCHASTIC_TOLERANCE}. */
	protected final static double STOCHASTIC_TOLERANCE = 1E-6;

	/** The graph. */
	public final ImmutableGraph graph;
	/** The number of nodes of {@link #graph}, cached. */
	public final long n;
	/** A logger defined by the implementing subclasses. */
	public final Logger logger;
	/** The current rank vector. */
	public double[][] rank;
	/** The current step (0 after {@linkplain #init() initialization}). */
	public int iteration;

	/** Creates a new instance.
	 *
	 * @param graph the graph.
	 * @param logger a logger.
	 */
	public SpectralRanking(final ImmutableGraph graph, final Logger logger) {
		this.graph = graph;
		this.logger = logger;
		this.n = graph.numNodes();
		logger.info("Nodes: " + n);
	}


	/** A a strategy that decides when a computation should be stopped. */
	public interface StoppingCriterion {
		/** Determines if the computation should be stopped.
		 *
		 * @param spectralRanking the instance incapsulating the computation.
		 * @return true if the computation should be stopped.
		 */
		public boolean shouldStop(SpectralRanking spectralRanking);
	};

	/** A stopping criterion that stops whenever the number of iterations exceeds a given bound. */
	public static class IterationNumberStoppingCriterion implements StoppingCriterion {
		private final int maxIter;
		/** Creates an instance with a given number of iterations.
		 *
		 * @param maxIter the maximum number of iterations.
		 */
		public IterationNumberStoppingCriterion(final int maxIter) {
			this.maxIter = maxIter;
		}

		@Override
		public boolean shouldStop(final SpectralRanking spectralRanking) {
			// If maxIter is infinity, we just return.
			if (maxIter == Integer.MAX_VALUE) return false;
			spectralRanking.logger.info("Iterations performed: " + spectralRanking.iteration + " (will stop after " + maxIter + ")");
			return spectralRanking.iteration >= maxIter;
		}
	}

	/** A stopping criterion that evaluates {@link SpectralRanking#normDelta()}, and stops
	 *  if this value is smaller than a given threshold.
	 *
	 *  <p>Note that this criterion assumes {@link SpectralRanking#normDelta()} has been properly implemented.
	 */
	public static class NormStoppingCriterion implements StoppingCriterion {
		private final double threshold;

		/** Creates an instance with given threshold.
		 *
		 * @param threshold the threshold.
		 */
		public NormStoppingCriterion(final double threshold) {
			this.threshold = threshold;
		}

		@Override
		public boolean shouldStop(final SpectralRanking spectralRanking) {
			spectralRanking.logger.info("Current norm delta: " + spectralRanking.normDelta() + " (will stop below " + threshold + ")");
			return spectralRanking.normDelta() < threshold;
		}
	}

	/** Composes two stopping criteria, producing a single stopping criterion (the computation stops iff both
	 *  conditions become true; lazy boolean evaluation is applied).
	 *
	 * 	@param stop1 a stopping criterion.
	 *  @param stop2 a stopping criterion.
	 *  @return a criterion that decides to stop as soon as both criteria are satisfied.
	 */
	public static StoppingCriterion and(final StoppingCriterion stop1, final StoppingCriterion stop2) {
		return p -> stop1.shouldStop(p) && stop2.shouldStop(p);
	}

	/** Composes two stopping criteria, producing a single stopping criterion (the computation stops iff either
	 *  condition becomes true; lazy boolean evaluation is applied).
	 *
	 *  @param stop1 a stopping criterion.
	 *  @param stop2 a stopping criterion.
	 *  @return a criterion that decides to stop as soon as one of the two criteria is satisfied.
	 */
	public static StoppingCriterion or(final StoppingCriterion stop1, final StoppingCriterion stop2) {
		return p -> stop1.shouldStop(p) || stop2.shouldStop(p);
	}

	/** Commodity method checking whether a vector is stochastic (nonnegative entries summing up to one within {@link #STOCHASTIC_TOLERANCE}).
	 *
	 * <p>This method uses <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan's summation algorithm</a>.
	 *
	 * @param v the vector to check.
	 * @return true if the vector is stochastic.
	 */
	protected static boolean isStochastic(final DoubleBigList v) {
		double normL1 = 0.0, c = 0.0, t, y;
		long i;
		//Kahan method to minimize the round errors in doubles sum.
		for (i = v.size64(); i-- != 0 && v.getDouble(i) >= 0;) {
			y = v.getDouble(i) - c;
			t = (normL1 + y);
			c = (t - normL1) - y;
			normL1 = t;
		}
		return (i == -1 && Math.abs(normL1 - 1.0) <= STOCHASTIC_TOLERANCE);
	}

	/** Returns a {@link Properties} object that contains all parameters used by the computation.
	 *
	 * <p>Implementing subclasses should extends this method by calling <code>super()</code>
	 * and setting additional properties on the resulting {@link Properties}.
	 *
	 * @param graphBasename basename of the graph
	 * @return a properties object that represent all the parameters used to calculate the ranking.
	 */
	public Properties buildProperties(final String graphBasename) {
		final Properties prop = new Properties();
		prop.setProperty("iterations", iteration);
		prop.setProperty("normdelta", Double.toString(normDelta()));
		prop.setProperty("nodes", n);
		prop.setProperty("graph", graphBasename);
		return prop;
	}

	/** Initializes the rank vector, zeroes {@link #iteration} and logs basic data. Please extend this method to handle additional attributes. */
	@SuppressWarnings("unused")
	public void init() throws IOException {
		logger.info("Initializing...");
		iteration = 0;
		// Creates the array, if necessary
		if (rank == null) rank = DoubleBigArrays.newBigArray(n);
	}


	/** Performs one computation step. */
	public abstract void step() throws IOException;

	/** Returns the norm of an estimation of the distance to the limit of the iterative process: depending
	 * on the implementation, this can be an actual bound or, for example, just the difference between the
	 * last two approximations.
	 *
	 * <p>This method must be implemented by concrete subclasses if you want to use {@link NormStoppingCriterion}.
	 *
	 * @return the norm of an estimation of the distance to the limit.
	 * @throws IllegalStateException if called before the first iteration.
	 * @throws UnsupportedOperationException if it is not possible to compute a norm.
	 */
	public double normDelta() {
		throw new UnsupportedOperationException();
	}

	/** Calls {@link #init()} and steps until a given stopping criterion is met.
	 * The criterion is checked <i>a posteriori</i> (i.e., after each step); this means that
	 * at least one step is performed.
	 *
	 * @param stoppingCriterion the stopping criterion to be used.
	 */
	public void stepUntil(final StoppingCriterion stoppingCriterion) throws IOException {
		init();
		do step(); while (!stoppingCriterion.shouldStop(this));
	}

	/** Clears all data and releases resources by nulling {@link #rank} (i.e., results we no longer be available).
	 * Please extend this method to handle additional attributes. */
	public void clear() {
		rank = null;
	}

	/** Returns a compact logarithmic approximation of a norm vector.
	 *
	 * @param doubleIterator an iterator enumerating a norm vector.
	 * @return an array of bytes containing the opposite of a lower bound on the binary logarithm of the doubles returned by the iterator.
	 */
	protected byte[][] approximateNormVector(final DoubleIterator doubleIterator) {
		final byte[][] normVector = ByteBigArrays.newBigArray(n);

		for (long i = 0; i < n; i++) {
			final double e = doubleIterator.nextDouble();
			if (e == 0) throw new IllegalArgumentException("A norm vector cannot contain zeroes");
			if (e > 1) throw new IllegalArgumentException("The norm vector contains an entry larger than one: " + e);
			final int approx = (int)Math.ceil(- Math.log(e) / Math.log(2));
			if (approx > 62) throw new IllegalArgumentException("The norm vector has an entry smaller than 1/2^62 (" + e + ")");
			ByteBigArrays.set(normVector, i, (byte) approx);
		}

		return normVector;
	}

}

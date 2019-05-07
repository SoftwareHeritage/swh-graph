package it.unimi.dsi.law.vector;

/*
 * Copyright (C) 2008-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */


import java.io.Serializable;

//RELEASE-STATUS: DIST

/** A class representing a vector of <code>double</code>. Different implementation can allow mutable or immutable values
 * through suitably implemented {@link #set(int, double)} and {@link #get(int)} method. Immutable implementation should throw an
 * {@link java.lang.UnsupportedOperationException} if a method that cause a mutation is called.
 */
public abstract class Vector implements Serializable {
	private static final long serialVersionUID = 1L;

	/** A value indicating that the norm is not computed for current values. */
	public static final int INVALID_ID = -1;

	/** A value indicating that the norm is not computed for current values. */
	public static final double INVALID_NORM = -1.0;

	/** The vector size (immutable). */
	public final int size;

	/** The description ID associated with this vector (immutable). */
	public final int id;

	/** The value of computed norm. It can be {@link #INVALID_NORM} if the norm is not computer for current values. */
	protected double ell2norm;

	/** The value of  computed L1 norm. */
	protected double ell1norm;

	/** The mutability status of this vector. */
	private final boolean mutable;

	/** Build a vector of given size and set the mutability status of this vector.
	 *
	 * @param size the size.
	 * @param mutable the mutability status.
	 * @param id the id of description of this vector.
	 */
	protected Vector (final int size, final boolean mutable, final int id) {
		this.size = size;
		this.mutable = mutable;
		this.id = id;
		ell2norm = ell1norm = INVALID_NORM;
	}

	/** Sets the value <var>val</var> at index <var>idx</var>.
	 *
	 * @param idx the index.
	 * @param val the value.
	 */
	public abstract void set (final int idx, final double val);

	/** Gets the value at index <var>idx</var>.
	 *
	 * @param idx the index.
	 * @return the value at index <var>idx</var>.
	 */
	public abstract double get (final int idx);

	/** Adds values in vector <var>v</var> scaled by <var>alpha</var> to this vector.
	 *
	 * @param alpha the scaling factor.
	 * @param v the vector to add.
	 */
	public void add (final double alpha, final Vector v) {
		// check size
		if (size != v.size)
			throw new IllegalArgumentException ("vectors with different size");

		for(int i = size; i-- != 0;)
			set (i, get(i) + v.get(i) * alpha);

		ell2norm = INVALID_NORM;
	}

	/** Scale values in this vector by a value <var>alpha</var>.
	 *
	 * @param alpha the scaling factor
	 */
	public void scale (final double alpha) {
		for(int i = size; i-- != 0;)
			set (i, alpha * get (i));

		// update norm
		if (ell2norm != INVALID_NORM)
			ell2norm *= Math.abs (alpha);
		if (ell1norm != INVALID_NORM)
			ell1norm *= Math.abs(alpha);
	}

	/** Reset (to zero) this vector.
	 *
	 *
	 */
	public void zero () {
		for (int i = 0; i < size; i++)
			set (i, 0.0);

		ell2norm = ell1norm = 0.0;				// update norm
	}

	/** Returns the mutability status of this vector.
	 *
	 * @return <code>true</code> if the vector is mutable; <code>false</code> otherwise.
	 */
	public boolean isMutable () {
		return mutable;
	}

	/** Returns the dot product between <var>v</var> and this vector.
	 *
	 * @param v the vector.
	 * @return the dot product.
	 */
	public double dotProduct (final Vector v) {
		// check size
		if (size != v.size)
			throw new IllegalArgumentException ("vectors with different size");

		// compute dot product
		double dot = 0.0;

		double c = 0.0, t, y;
		for(int i = size; i-- != 0;) {
			y = (get(i) * v.get(i)) - c;
			t = dot + y;
	        c = (t - dot) - y;
	        dot = t;

			//dot += get (i) * v.get(i);
		}

		return dot;
	}

	/** Returns the euclidean distance between <var>v</var> and this vector.
	 *
	 * @param v the vector.
	 * @return the euclidean distance.
	 */
	public double euclideanDistance (final Vector v) {
		// check size
		if (size != v.size)
			throw new IllegalArgumentException ("vectors with different size");

		// compute distance
		double dist = 0.0, temp;

		for(int i = size; i-- != 0;) {
			temp = get(i) - v.get(i);
			dist += temp * temp;
		}

		return Math.sqrt (Math.abs (dist));
	}

	/** Returns the l<sub>2</sub> norm of this vector.
	 *
	 * @return the l<sub>2</sub> norm.
	 */
	public double ell2Norm () {
		if (ell2norm == INVALID_NORM)
			ell2norm = Math.sqrt (Math.abs (dotProduct(this)));		// just a shortcut

		return ell2norm;
	}

	/** Returns the l<sub>1</sub> norm of this vector.
	 */
	public double ell1Norm() {
		if (ell1norm == INVALID_NORM)
		{
			double tempNorm = 0.0;
			for(int i = size; i-- != 0;)
				tempNorm += Math.abs (get(i));
			ell1norm = tempNorm;
		}
		return ell1norm;
	}


}

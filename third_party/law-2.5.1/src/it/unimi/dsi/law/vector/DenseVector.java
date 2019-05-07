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

import java.util.Arrays;

// RELEASE-STATUS: DIST

/** A mutable implementation of {@link Vector} optimized for dense vectors. */
public class DenseVector extends Vector {

	static final long serialVersionUID = 2006002L;

	/** An arrays containing vector values. */
	final double[] value;

	/**
	 * Build a vector of given size with zero values.
	 *
	 * @param size the size.
	 * @param id the id of description of this vector.
	 */
	private DenseVector(final int size, final int id) {
		super(size, true, id);
		this.value = new double[size];
		ell2norm = ell1norm = 0.0;
	}

	/**
	 * Build a vector from an array of values.
	 *
	 * @param value an array of values.
	 * @param id the id of description of this vector.
	 */
	private DenseVector(final double[] value, final int id) {
		super(value.length, true, id);
		this.value = value;
		ell2norm = ell1norm = INVALID_NORM;
	}

	/**
	 * Returns an instance of given size with zero values.
	 *
	 * @param size the size.
	 * @param id the id of description of this vector.
	 * @return a vector.
	 */
	public static DenseVector getInstance(final int size, final int id) {
		return new DenseVector(size, id);
	}

	/**
	 * Returns an instance from an array of values.
	 *
	 * @param value an array of values.
	 * @param id the id of description of this vector.
	 * @return a vector.
	 */
	public static DenseVector getInstance(final double[] value, final int id) {
		return new DenseVector(value, id);
	}

	public void set(final int idx, final double val) {
		if (idx < 0 || idx >= size) throw new IllegalArgumentException("index out of range");

		if (ell1norm != INVALID_NORM) ell1norm = ell1norm - Math.abs(value[idx]) + Math.abs(val);

		value[idx] = val;
		ell2norm = INVALID_NORM;
	}

	public double get(final int idx) {
		if (idx < 0 || idx >= size) throw new IllegalArgumentException("index out of range");

		return value[idx];
	}

	public void add(final double alpha, final Vector v) {
		// check size
		if (size != v.size) throw new IllegalArgumentException("vectors with different size");

		if (v instanceof DenseVector) {
			final double[] dvValue = ((DenseVector)v).value;

			for (int i = size; i-- != 0;)
				value[i] += dvValue[i] * alpha;
		}
		else {
			if (v instanceof ImmutableSparseVector) {
				ImmutableSparseVector sv = (ImmutableSparseVector)v;
				final double[] svValue = sv.value;
				final int[] svIndex = sv.index;

				for (int i = sv.nonZero; i-- != 0;)
					value[svIndex[i]] += svValue[i] * alpha;
			}
			else super.add(alpha, v);
		}

		ell2norm = ell1norm = INVALID_NORM;
	}

	public void scale(final double alpha) {
		for (int i = size; i-- != 0;)
			value[i] *= alpha;

		// update norm
		if (ell2norm != INVALID_NORM) ell2norm *= Math.abs(alpha); // update norm
		if (ell1norm != INVALID_NORM) ell1norm *= Math.abs(alpha);
	}

	public void zero() {
		Arrays.fill(value, 0.0);
		ell2norm = 0.0; // update norm
	}

	public double dotProduct(final Vector v) {
		// check size
		if (size != v.size) throw new IllegalArgumentException("vectors with different size");

		// compute dot product
		if (v instanceof DenseVector) {
			final double[] dvValue = ((DenseVector)v).value;

			double dot = 0.0;

			double c = 0.0, t, y;
			for (int i = size; i-- != 0;) {
				y = (value[i] * dvValue[i]) - c;
				t = dot + y;
				c = (t - dot) - y;
				dot = t;

				// dot += value[i] * dvValue[i];
			}

			return dot;
		}
		else if (v instanceof ImmutableSparseVector) return v.dotProduct(this);
		else return super.dotProduct(v);
	}

	public double euclideanDistance(final Vector v) {
		// check size
		if (size != v.size) throw new IllegalArgumentException("vectors with different size");

		// compute distance
		if (v instanceof DenseVector) {
			final double[] dvValue = ((DenseVector)v).value;

			double dist = 0.0, temp;

			double c = 0.0, t, y;
			for (int i = size; i-- != 0;) {
				temp = value[i] - dvValue[i];
				y = (temp * temp) - c;
				t = dist + y;
				c = (t - dist) - y;
				dist = t;

				// temp = value[i] - dvValue[i];
				// dist += temp * temp;
			}

			return Math.sqrt(Math.abs(dist));
		}
		else if (v instanceof ImmutableSparseVector) return Math.sqrt(Math.abs(dotProduct(this) + v.dotProduct(v) - 2 * v.dotProduct(this)));
		else return super.euclideanDistance(v);
	}

	public double ell2Norm() {
		if (ell2norm == INVALID_NORM) {
			double tempNorm = 0.0;
			for (int i = size; i-- != 0;)
				tempNorm += value[i] * value[i];

			ell2norm = Math.sqrt(Math.abs(tempNorm));
		}

		return ell2norm;
	}

	@Override
	public double ell1Norm() {
		if (ell1norm == INVALID_NORM)
		{
			double tempNorm = 0.0;
			for (int i = size; i-- != 0 ;)
				tempNorm += Math.abs(value[i]);
			ell1norm = tempNorm;
		}

		return ell1norm;
	}

}

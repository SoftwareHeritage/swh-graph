package it.unimi.dsi.law.vector;

import java.util.Arrays;

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


import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

//RELEASE-STATUS: DIST

/** An immutable implementation of {@link Vector} optimized for sparse vectors. */
public class ImmutableSparseVector extends Vector {

	static final long serialVersionUID = 2006002L;

	/** An arrays containing vector values. */
	final public double[] value;

	/** An arrays containing the indexes of the vector values. */
	final public int[] index;

	/** The number of non-zero entry in this vector. */
	final public int nonZero;

	/** The index in {@link #index} of the last value returned by {@link #get(int)}. */
	private int lastIndex;

	/** The value of <code>index[lastIdx]</code>. */
	private int lastIndexValue;

	/** The value of <code>index[lastIdx + 1]</code> (or <code>size</code> if <code>lastIdx == nonZero - 1</code>. */
	private int  lastIndexNextValue;

	// ALERT: document that index is supposed to be SORTED!

	/** Build a vector of given size from an array of values.
	 *
	 * @param size the size.
	 * @param value the value.
	 * @param index the index.
	 * @param id the id of description of this vector.
	 */
	private ImmutableSparseVector (final int size, final double[] value, final int[] index, final int id) {
		super (size, false, id);
		this.value = value;
		this.index = index;
		this.nonZero = index.length;
		// reset last index
		lastIndex = lastIndexValue = lastIndexNextValue = -1;
	}

	/** Returns an instance of given size from an array of values.
	 *
	 * @param size the size.
	 * @param value the value.
	 * @param index the index.
	 * @param id the id of description of this vector.
	 * @return an immutable vector.
	 */
	public static ImmutableSparseVector getInstance (final int size, final double[] value, final int[] index, final int id) {
		// check for different length
		if (value.length != index.length)
			throw new IllegalArgumentException ("array with different size");

		// check for negative value in index
		for(int i = index.length; i-- != 0;)
			if (index[i] < 0)
				throw new IllegalArgumentException ("index with negative value");

		return new ImmutableSparseVector (size, value, index, id);
	}

	/** Returns an instance containing all the values of a given vector larger or smaller of a given threshold.
	 * The vector will have size equal to the vector length.
	 *
	 * @param v the vector.
	 * @param threshold the threshold.
	 * @param id the id of description of this vector.
	 * @return an immutable vector
	 */
	public static ImmutableSparseVector getInstance (final Vector v, final double threshold, final int id) {
		final IntArrayList indexList = new IntArrayList();
		final DoubleArrayList valueList = new DoubleArrayList();
		final int size = v.size;

		// add value grater than threshold
		for(int i = size; i-- != 0;) {
			final double d = v.get(i);
			if (d < -threshold || d > threshold) {
				indexList.add (i);
				valueList.add (d);
			}
		}

		return new ImmutableSparseVector (size, valueList.toDoubleArray(), indexList.toIntArray(), id);
	}

	/** Returns an instance containing all the values of a given array larger or smaller of a given threshold.
	 * The vector will have size equal to the array length.
	 *
	 * @param value tha array of double.
	 * @param threshold the threshold.
	 * @param id the id of description associated with this vector.
	 * @return an immutable vector.
	 */
	public static ImmutableSparseVector getInstance (final double[] value, final double threshold, final int id) {
		final IntArrayList indexList = new IntArrayList();
		final DoubleArrayList valueList = new DoubleArrayList();
		final int size = value.length;

		// add value grater than threshold
		for (int i = 0; i < size; i++) {
			final double d = value[i];
			if (d < -threshold || d > threshold) {
				indexList.add (i);
				valueList.add (d);
			}
		}

		return new ImmutableSparseVector (size, valueList.toDoubleArray(), indexList.toIntArray(), id);
	}

	/** Returns an instance containing all the values of a given array larger than a given threshold.
	 * The vector will have the given size
	 *
	 * @param value tha array of double.
	 * @param threshold the threshold.
	 * @param id the id of description associated with this vector.
	 * @return an immutable vector.
	 */
	public static ImmutableSparseVector getInstance (final int[] index, final double[] value, final int size, final  double threshold, final int id) {
		final IntArrayList indexList = new IntArrayList();
		final DoubleArrayList valueList = new DoubleArrayList();

		// add value grater than threshold
		for (int i = 0; i < index.length; i++) {

			if (value[i] >= threshold)
			{
				indexList.add (index [i]);
				valueList.add (value [i]);
			}
		}

		return new ImmutableSparseVector (size, valueList.toDoubleArray(), indexList.toIntArray(), id);
	}

	public void set (final int idx, final double val) {
		throw new UnsupportedOperationException ("ImmutableSparseVector is immutable");
	}

	public double get (final int idx) {
		if (idx < 0 || idx >= size)
			throw new IllegalArgumentException ("index out of range");
		// shortcut
		if (idx > lastIndexValue && idx < lastIndexNextValue)	// beetwen indeces
			return 0.0;
		if (idx == lastIndexNextValue) {									// next index
			lastIndex++;
			lastIndexValue = index[lastIndex];
			lastIndexNextValue = lastIndex == nonZero - 1 ? size : index[lastIndex + 1];
			return value[lastIndex];
		}

		// shortcut failed...take the long way
		int pos = Arrays.binarySearch(index, idx);
		if (pos < 0) {				// value not found -> reset
			lastIndexValue = lastIndexNextValue = -1;
			return 0.0;
		}

		lastIndex = pos;
		lastIndexValue = index[lastIndex];
		lastIndexNextValue = lastIndex == nonZero - 1 ? size : index[lastIndex + 1];
		return value[pos];
	}

	public void add (final double alpha, final Vector v) {
		throw new UnsupportedOperationException ("ImmutableSparseVector is immutable");
	}

	public void scale (final double alpha) {
		throw new UnsupportedOperationException ("ImmutableSparseVector is immutable");
	}

	public void zero () {
		throw new UnsupportedOperationException ("ImmutableSparseVector is immutable");
	}

	public double dotProduct (final Vector v) {
		// check size
		if (size != v.size)
			throw new IllegalArgumentException ("vectors with different size");

		// compute dot product
		if (v instanceof ImmutableSparseVector) {
			final ImmutableSparseVector sVector = (ImmutableSparseVector) v;
			final double[] svValue = sVector.value;
			final int[] svIndex = sVector.index;
			final int last0 = nonZero, last1 = sVector.nonZero;

			int i = 0, j = 0;
			double dot = 0.0;

			double c = 0.0, t, y;
			while (i < last0 && j < last1) {
				if (index[i] < svIndex[j])
					i++;
				else {
					if (svIndex[j] < index[i])
						j++;
					else {
						y = (value[i] * svValue[j]) - c;
						t = dot + y;
				        c = (t - dot) - y;
				        dot = t;

						//dot += value[i] * svValue[j];
						i++;
						j++;
					}
				}
			}

			return dot;
		}
		else if (v instanceof DenseVector) {
			final double[] dvValue = ((DenseVector)v).value;

			double dot = 0.0;
			double c = 0.0, t, y;

			for(int i = nonZero; i-- != 0;) {
				y = (dvValue[index[i]] * value[i]) - c;
				t = dot + y;
				c = (t - dot) - y;
				dot = t;

				//dot += dvValue[index[i]] * value[i];
			}

			return dot;
		}
		else
			return super.dotProduct (v);
	}

	public double euclideanDistance (final Vector v) {
		// check size
		if (size != v.size)
			throw new IllegalArgumentException ("vectors with different size");

		// compute distance
		if (v instanceof DenseVector)
			return v.euclideanDistance(this);
		else if (v instanceof ImmutableSparseVector)
			return Math.sqrt (Math.abs (dotProduct (this) + v.dotProduct (v) - 2 * dotProduct (v)));
		else
			return super.euclideanDistance (v);
	}

	public double ell2Norm () {
		if (ell2norm == INVALID_NORM) {
			double tempNorm = 0.0;
			int i = nonZero;
			while (--i >= 0)
				tempNorm += value[i] * value[i];
			ell2norm = Math.sqrt (Math.abs (tempNorm));
		}

		return ell2norm;
	}

	public double ell1Norm() {
		if (ell1norm == INVALID_NORM) {
			double tempNorm = 0.0;
			for(int i = nonZero; i-- != 0 ;)
				tempNorm += Math.abs(value[i]);
			ell1norm = tempNorm;
		}

		return ell1norm;
	}

}

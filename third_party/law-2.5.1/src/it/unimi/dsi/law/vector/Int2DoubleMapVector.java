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


import it.unimi.dsi.fastutil.ints.Int2DoubleAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;

//RELEASE-STATUS: DIST

/** A mutable implementation of {@link Vector} for sparse vectors.
 * It is possible to create sparse vector by iteratively adding values. This implementation uses the supertype method for every operation.
 * It should be converted with {@link #toImmutableSparseVector()} before being actually used.
 */
public class Int2DoubleMapVector extends Vector {

	static final long serialVersionUID = 2006002L;

	/** Storage for vector values. */
	private final Int2DoubleMap value;

	/** Build a vector of given size with zero values.
	 *
	 * @param size the size.
	 * @param id the id of description of this vector.
	 */
	private Int2DoubleMapVector (final int size, final int id) {
		super (size, true, id);
		value = new Int2DoubleAVLTreeMap();
		ell2norm = ell1norm = 0.0;
	}

	/** Build a vector from the given {@link Int2DoubleMap}.
	 *
	 * @param size the size.
	 * @param value a map of values.
	 * @param id the id of description of this vector.
	 */
	private Int2DoubleMapVector (final int size, final Int2DoubleMap value, final int id) {
		super (size, true, id);
		this.value = value;
		ell2norm = INVALID_NORM;
	}

	/** Returns an instance of given size with zero values.
	 *
	 * @param size the size.
	 * @param id the id of description of this vector.
	 * @return a vector
	 */
	public static Int2DoubleMapVector getInstance (final int size, final int id) {
		return new Int2DoubleMapVector (size, id);
	}

	/** Returns an instance from the given {@link Int2DoubleMap}.
	 *
	 * @param size the size.
	 * @param value a map of values.
	 * @param id the id of description of this vector.
	 * @return a vector.
	 */
	public static Int2DoubleMapVector getInstance (final int size, final Int2DoubleMap value, final int id) {
		return new Int2DoubleMapVector (size, value, id);
	}

	public void set (final int idx, final double val) {
		if (idx < 0 || idx >= size)
			throw new IllegalArgumentException ("index out of range");

		value.put (idx, val);
		ell2norm =  ell1norm = INVALID_NORM;
	}

	public double get (final int idx) {
		if (idx < 0 || idx >= size)
			throw new IllegalArgumentException ("index out of range");

		return value.get (idx);
	}

	public ImmutableSparseVector toImmutableSparseVector () {
		final double[] value = (this.value.values()).toDoubleArray();
		final int[] index = (this.value.keySet()).toIntArray();

		return ImmutableSparseVector.getInstance (size, value, index, id);
	}

}

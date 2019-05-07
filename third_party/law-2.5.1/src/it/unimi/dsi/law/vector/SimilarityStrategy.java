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

/** An interface specifying methods used to obtain pattern similarities. */
public interface SimilarityStrategy extends Serializable{

	/** Returns the similarity value between two vectors.
	 *
	 * @param v0 the first vector.
	 * @param v1 the second vector.
	 * @return the similarity between vectors.
	 */
	public double similarity (Vector v0, Vector v1);

}

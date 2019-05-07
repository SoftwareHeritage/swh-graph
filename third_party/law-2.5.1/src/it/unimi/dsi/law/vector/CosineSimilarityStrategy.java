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

//RELEASE-STATUS: DIST

/** A class that compute the similarity between pattern using cosine similarity. */
public class CosineSimilarityStrategy implements SimilarityStrategy {

	static final long serialVersionUID = 2006001L;

	public double similarity(Vector v0, Vector v1) {
		double dot = v0.dotProduct(v1);
		return dot == 0.0 ? 0.0 : dot / (v0.ell2Norm() * v1.ell2Norm());
	}

}

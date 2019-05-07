package it.unimi.dsi.law.warc.util;

/*
 * Copyright (C) 2004-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

// RELEASE-STATUS: DIST

/**
 * Allows to determine if an {@link HttpResponse} is duplicate.
 *
 * For more details, see the relative <a href='../io/package-summary.html#dup'>section</a> in the <code>it.unimi.dsi.law.warc.io</code> package description.
 */
public interface DigestBasedDuplicateDetection {


	/** Returns the content digest.
	 *
	 * @return the digest.
	 */
	public byte[] digest();

	/** Returns the duplicate status of this response.
	 *
	 * @return whether this response is a duplicate.
	 */
	public boolean isDuplicate();

}

package it.unimi.dsi.law.warc.filters;

import java.net.URI;

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

import it.unimi.dsi.law.bubing.util.BURL;

// RELEASE-STATUS: DIST

/** Accepts only a given URIs. */
public class URLEquals extends AbstractFilter<URI> {

	/** The URL to be matched. */
	private final URI uri;

	/** Creates a filter that only accepts URIs equal to a given URI.
	 *
	 * @param uri a URI.
	 */
	public URLEquals(final String uri) {
		this.uri = BURL.parse(uri);
		if (this.uri == null) throw new IllegalArgumentException("Unparsable URI " + uri);
	}

	public boolean apply(final URI uri) {
		return uri.equals(uri);
	}

	public static URLEquals valueOf(String spec) {
		return new URLEquals(spec);
	}

	public String toString() {
		return toString(uri);
	}

	public boolean equals(Object x) {
		if (x instanceof URLEquals) return ((URLEquals)x).uri.equals(uri);
		else return false;
	}
}

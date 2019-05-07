package it.unimi.dsi.law.warc.filters;

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

import java.net.URI;

// RELEASE-STATUS: DIST

/** Accepts only URIs whose scheme equals a certain string (typically, <code>http</code>).
 *
 * <p>Note that {@link #apply(URI)} will throw an {@link IllegalArgumentException}
 * if the argument has a {@code null} {@linkplain URI#getScheme() scheme}.
 */
public class SchemeEquals extends AbstractFilter<URI> {

	/** The accepted scheme. */
	private final String scheme;

	/** Creates a filter that only accepts URIs with a given scheme.
	 *
	 * @param scheme the accepted scheme.
	 */
	public SchemeEquals(final String scheme) {
		this.scheme = scheme;
	}

	public boolean apply(final URI uri) {
		if (uri.getScheme() == null) throw new IllegalArgumentException("URI \"" + uri + "\" has no scheme");
		return scheme.equals(uri.getScheme());
	}

	public static SchemeEquals valueOf(String spec) {
		return new SchemeEquals(spec);
	}

	public String toString() {
		return toString(scheme);
	}

	public boolean equals(Object x) {
		if (x instanceof SchemeEquals) return ((SchemeEquals)x).scheme.equals(scheme);
		else return false;
	}
}

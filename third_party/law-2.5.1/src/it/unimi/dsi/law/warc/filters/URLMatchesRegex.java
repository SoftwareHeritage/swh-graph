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
import java.util.regex.Pattern;

// RELEASE-STATUS: DIST

/** Accepts only URIs that match a certain regular expression. */
public class URLMatchesRegex extends AbstractFilter<URI> {

	/** The pattern containing the compiled regular expression. */
	private Pattern pattern;

	/** Creates a filter that only accepts URLs matching a given regular expression.
	 *
	 * @param expr the regular expression.
	 */
	public URLMatchesRegex(final String expr) {
		pattern = Pattern.compile(expr);
	}

	public boolean apply(final URI uri) {
		return pattern.matcher(uri.toString()).matches();
	}

	public static URLMatchesRegex valueOf(String spec) {
		return new URLMatchesRegex(spec);
	}

	public String toString() {
		return toString(pattern.pattern());
	}

	public boolean equals(Object x) {
		if (x instanceof URLMatchesRegex) return ((URLMatchesRegex)x).pattern.equals(pattern);
		else return false;
	}
}

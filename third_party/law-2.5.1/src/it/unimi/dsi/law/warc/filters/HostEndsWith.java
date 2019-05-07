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

/** Accepts only URIs whose host ends with (case-insensitively) a certain suffix.
 *
 *  <p>Note that {@link #apply(URI)} will throw an {@link IllegalArgumentException}
 *  if the argument has a null {@linkplain URI#getHost() host}.
 */
public class HostEndsWith extends AbstractFilter<URI> {

	/** The accepted host suffix (lowercased). */
	private final String suffix;

	/** Creates a filter that only accepts URLs with a given suffix.
	 *
	 * @param suffix the accepted suffix.
	 */
	public HostEndsWith(final String suffix) {
		this.suffix = suffix.toLowerCase();
	}

	public boolean apply(final URI uri) {
		if (uri.getHost() == null) throw new IllegalArgumentException("URI \"" + uri + "\" has no host");
		// BURL hosts are always lower cased
		return uri.getHost().endsWith(suffix);
	}

	public static HostEndsWith valueOf(String spec) {
		return new HostEndsWith(spec);
	}

	public String toString() {
		return toString(suffix);
	}

	public boolean equals(Object x) {
		if (x instanceof HostEndsWith) return ((HostEndsWith)x).suffix.equals(suffix);
		else return false;
	}
}

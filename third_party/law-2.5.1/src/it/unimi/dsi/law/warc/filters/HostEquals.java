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

/** Accepts only URIs whose host equals (case-insensitively) a certain string.
 *
 *  <p>Note that {@link #apply(URI)} will throw an {@link IllegalArgumentException}
 *  if the argument has a {@code null} {@linkplain URI#getHost() host}.
 */
public class HostEquals extends AbstractFilter<URI> {

	/** The accepted host. */
	private final String host;

	/** Creates a filter that only accepts URLs with a given host.
	 *
	 * @param host the accepted host.
	 */
	public HostEquals(final String host) {
		this.host = host;
	}

	public boolean apply(final URI uri) {
		if (uri.getHost() == null) throw new IllegalArgumentException("URI \"" + uri + "\" has no host");
		// BURL hosts are always lower cased
		return uri.getHost().equals(host);
	}

	public static HostEquals valueOf(String spec) {
		return new HostEquals(spec);
	}

	public String toString() {
		return toString(host);
	}

	public boolean equals(Object x) {
		if (x instanceof HostEquals) return ((HostEquals)x).host.equals(host);
		else return false;
	}

}

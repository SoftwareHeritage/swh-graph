package it.unimi.dsi.law.warc.filters;

import com.google.common.net.HttpHeaders;

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

import it.unimi.dsi.law.warc.util.HttpResponse;

// RELEASE-STATUS: DIST

/** Accepts only fetched response whose content type starts with a given string.
 *
 *  <p>Typical usage: <code>ContentTypeStartsWith(text/)</code>,
 */
public class ContentTypeStartsWith extends AbstractFilter<HttpResponse> {
	/** The prefix of accepted content types. */
	private final String prefix;

	public ContentTypeStartsWith(final String prefix) {
		this.prefix = prefix;
	}

	public boolean apply(HttpResponse x) {
		final String header = x.headers().get(HttpHeaders.CONTENT_TYPE);
		return header != null && header.startsWith(prefix);
	}

	public static ContentTypeStartsWith valueOf(String spec) {
		return new ContentTypeStartsWith(spec);
	}

	public String toString() {
		return toString(prefix);
	}

	public boolean equals(Object x) {
		return x instanceof ContentTypeStartsWith && ((ContentTypeStartsWith)x).prefix.equals(prefix);
	}
}

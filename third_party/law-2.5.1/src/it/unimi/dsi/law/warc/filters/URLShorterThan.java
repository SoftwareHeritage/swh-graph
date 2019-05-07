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

/** Accepts only URIs whose overall length is below a given threshold. */
public class URLShorterThan extends AbstractFilter<URI> {

	/** URL longer than this threshold won't be accepted. */
	private final int threshold;

	/** Creates a filter that only accepts URLs shorter than the given threshold.
	 *
	 * @param threshold the acceptance threshold.
	 */
	public URLShorterThan(final int threshold) {
		this.threshold = threshold;
	}

	public boolean apply(final URI uri) {
		return uri.toString().length() < threshold;
	}

	public static URLShorterThan valueOf(String spec) {
		return new URLShorterThan(Integer.parseInt(spec));
	}

	public String toString() {
		return toString(Integer.toString(threshold));
	}

	public boolean equals(Object x) {
		return x instanceof URLShorterThan && ((URLShorterThan)x).threshold == threshold;
	}
}

package it.unimi.dsi.law.warc.filters;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.law.warc.util.Util;

// RELEASE-STATUS: DIST

/** Accepts only URIs whose path ends (case-insensitively) with one of a given set of suffixes. */
public class PathEndsWithOneOf extends AbstractFilter<URI> {

	/** The accepted suffixes, downcased. */
	private final String[] suffixes;

	/** Creates a filter that only accepts URLs whose path ends with one of a given set of suffixes.
	 *
	 * @param suffixes the accepted suffixes.
	 */
	public PathEndsWithOneOf(final String[] suffixes) {
		this.suffixes = new String[suffixes.length];
		for (int i = 0; i < suffixes.length; i++) this.suffixes[i] = suffixes[i].toLowerCase();
	}

	@Override
	public boolean apply(final URI uri) {
		String file = uri.getRawPath().toLowerCase();
		for (String suffix: suffixes) if (file.endsWith(suffix)) return true;
		return false;
	}

	public static PathEndsWithOneOf valueOf(String spec) throws IOException {
		return new PathEndsWithOneOf(Util.parseCommaSeparatedProperty(spec));
	}

	public String toString() {
		return toString((Object[])suffixes);
	}

	public boolean equals(Object x) {
		if (x instanceof PathEndsWithOneOf) {
			Set<String> suffixSet = new ObjectOpenHashSet<String>(suffixes);
			Set<String> xSuffixSet = new ObjectOpenHashSet<String>(((PathEndsWithOneOf)x).suffixes);
			return suffixSet.equals(xSuffixSet);
		}
		else return false;
	}
}

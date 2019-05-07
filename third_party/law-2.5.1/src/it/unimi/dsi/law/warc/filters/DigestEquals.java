package it.unimi.dsi.law.warc.filters;

import java.util.Arrays;

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

import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.util.HttpResponse;
import it.unimi.dsi.law.warc.util.Util;

// RELEASE-STATUS: DIST

/** Accepts only records of given digest, specified as a hexadecimal string. */
public class DigestEquals extends AbstractFilter<WarcRecord> {
	private final byte[] digest;

	private DigestEquals(final byte[] digest) {
		this.digest = digest;
	}

	public boolean apply(final WarcRecord x) {
		String s = x.header.anvlFields.get(HttpResponse.DIGEST_HEADER);
		return s != null && Arrays.equals(digest, Util.fromHexString(s));
	}

	public static DigestEquals valueOf(final String spec) {
		return new DigestEquals(Util.fromHexString(spec));
	}

	public String toString() {
		return toString(Util.toHexString(digest));
	}

}

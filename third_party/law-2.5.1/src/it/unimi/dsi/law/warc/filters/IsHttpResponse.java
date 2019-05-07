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

import it.unimi.dsi.law.warc.io.WarcRecord;

// RELEASE-STATUS: DIST

/** Accepts only records that are http/https responses. */
public class IsHttpResponse extends AbstractFilter<WarcRecord> {

	public final static IsHttpResponse INSTANCE = new IsHttpResponse();

	private IsHttpResponse() {}

	public boolean apply(final WarcRecord x) {
		WarcRecord.Header header = x.header;
		return (header.recordType == WarcRecord.RecordType.RESPONSE &&
				(header.contentType == WarcRecord.ContentType.HTTP ||
					header.contentType == WarcRecord.ContentType.HTTPS));
	}

	public static IsHttpResponse valueOf(final String emptySpec) {
		if (emptySpec.length() > 0) throw new IllegalArgumentException();
		return INSTANCE;
	}

	public String toString() {
		return getClass().getSimpleName() + "()";
	}
}

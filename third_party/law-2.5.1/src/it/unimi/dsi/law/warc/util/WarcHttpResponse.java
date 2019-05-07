package it.unimi.dsi.law.warc.util;

import java.io.IOException;
import java.util.Map;

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

import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.law.warc.filters.IsHttpResponse;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;

// RELEASE-STATUS: DIST

/** An {@link AbstractHttpResponse} implementation that reads the response
 * content from a WARC record (via the {@link #fromWarcRecord(WarcRecord)} method.
 *
 */
public class WarcHttpResponse extends MetadataHttpResponse implements DigestBasedDuplicateDetection {

	private MeasurableInputStream block;
	private boolean headersHaveBeenParsed;

	private byte[] digest;
	private boolean isDuplicate;

	@Override
	public Map<String, String> headers() {
		ensureHeadersHabeBeenParsed();
		return headerMap;
	}

	public MeasurableInputStream contentAsStream() {
		ensureHeadersHabeBeenParsed();
		return block;
	}

	public boolean fromWarcRecord(WarcRecord wr) throws IOException {
		if (! IsHttpResponse.INSTANCE.apply(wr)) return false;
		uri = wr.header.subjectUri;
		final String digestAsString = wr.header.anvlFields.get(HttpResponse.DIGEST_HEADER);
		digest =  digestAsString != null ? Util.fromHexString(digestAsString) : null;
		isDuplicate = Boolean.valueOf(wr.header.anvlFields.get(HttpResponse.ISDUPLICATE_HEADER)).booleanValue();
		statusLine = Util.readStatusLine(wr.block, HttpResponse.HEADER_CHARSET);
		block = wr.block;
		headersHaveBeenParsed = false;
		return true;
	}

	private void ensureHeadersHabeBeenParsed() {
		if (block == null) throw new NullPointerException("Block not yet read");
		if (headersHaveBeenParsed) return;
		headerMap.clear();
		try {
			Util.readANVLHeaders(block, headerMap, HEADER_CHARSET);
		} catch (IOException | FormatException e) {
			throw new RuntimeException(e); // before we where swalloing the exeption in the caller
		}
		headersHaveBeenParsed = true;
	}

	public byte[] digest() {
		return digest;
	}

	public boolean isDuplicate() {
		return isDuplicate;
	}

}

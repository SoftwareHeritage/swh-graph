package it.unimi.dsi.law.bubing.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Random;

import org.apache.http.Header;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;

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

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.util.DigestBasedDuplicateDetection;
import it.unimi.dsi.law.warc.util.MetadataHttpResponse;

// RELEASE-STATUS: DIST

/** A class with mock implementations of responses. */

public class MockResponses {
	/** A response whose content is really generated at random. */
	static public class MockRandomHttpResponse extends MetadataHttpResponse {
		private final static int DEFAULT_MAX_CONTENT_LENGTH = 1024;
		private static int calls = 0;
		private final byte[] content;

		public MockRandomHttpResponse(final Random random) {
			this(random, DEFAULT_MAX_CONTENT_LENGTH);
		}

		@SuppressWarnings("deprecation")
		public MockRandomHttpResponse(final Random random, int maxContentLength) {
			uri = BURL.parse("http" + (random.nextBoolean() ? "s" : "") + "://this.is/n" + calls++ + "/test.html");
			statusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 0), 200, "OK");
			content = new byte[random.nextInt(maxContentLength) + 1];
			random.nextBytes(content);
			headerMap.clear();
			headerMap.put("content-type", "text/html");
		}

		public MeasurableInputStream expectedContentAsStream() {
			return new FastByteArrayInputStream(content);
		}

		public MeasurableInputStream contentAsStream() {
			return new FastByteArrayInputStream(content);
		}

		public boolean fromWarcRecord(WarcRecord wr) throws IOException {
			throw new UnsupportedOperationException();
		}
	}

	/** An HTTP response with given status, possibly some given headers, and a given content, extracted from a string. */
	public static class MockHttpResponseFromString extends MetadataHttpResponse implements DigestBasedDuplicateDetection {
		private String content;
		private boolean isDuplicate;
		private byte[] digest;

		public MockHttpResponseFromString(final StatusLine statusLine, final Header[] header, final URI uri, final String content) {
			this.statusLine = statusLine;
			this.uri = uri;
			this.headerMap.clear();
			headerMap.put("content-type", "text/html; charset=iso-8859-1");
			for (Header h: header) headerMap.put(h.getName(), h.getValue());
			this.content = content;
		}

		public MeasurableInputStream contentAsStream() {
			try {
				return new FastByteArrayInputStream(content.getBytes("ISO-8859-1"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		public boolean fromWarcRecord(WarcRecord wr) throws IOException {
			throw new UnsupportedOperationException();
		}

		public void digest(byte[] digest) {
			this.digest = digest;
		}

		public byte[] digest() {
			return digest;
		}

		public void isDuplicate(boolean isDuplicate) {
			this.isDuplicate = isDuplicate;
		}

		public boolean isDuplicate() {
			return isDuplicate;
		}
	}



}

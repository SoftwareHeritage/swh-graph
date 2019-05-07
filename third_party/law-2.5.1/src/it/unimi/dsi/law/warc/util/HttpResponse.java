package it.unimi.dsi.law.warc.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.StatusLine;

import com.google.common.base.Charsets;

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

// RELEASE-STATUS: DIST

/** Provides high level access to WARC records with <code>record-type</code> equal to
 * <code>response</code> and <code>content-type</code> equal to <code>HTTP</code>
 * (or <code>HTTPS</code>).
 */
public interface HttpResponse extends Response {

	/** The WARC <code>anvl-filed</code> name to store the charset recognized during parsing. */
	public static final String GUESSED_CHARSET_HEADER = "BUbiNG-guessed-charset";

	/** The WARC <code>anvl-filed</code> name to store the digest. */
	public static final String DIGEST_HEADER = "BUbiNG-content-digest";

	/** The WARC <code>anvl-filed</code> name to store the digest. */
	public static final String ISDUPLICATE_HEADER = "BUbiNG-is-duplicate";

	/** The {@link Charset} used to encode/decode the HTTP headers. */
	public static final Charset HEADER_CHARSET = Charsets.ISO_8859_1;

	/** Returns the response status.
	 *
	 * @return the status of this response.
	 */
	public int status();

	/** Returns the response status line.
	 *
	 * @return the status line of this response.
	 */
	public StatusLine statusLine();

	/** Returns the headers of this response.
	 *
	 * <p><strong>Warning</strong>: as of LAW 3.0, and contrarily to previous behaviour,
	 * the map is case-sensitive. Use the predefined names in {@link HttpHeaders} or
	 * {@link com.google.common.net.HttpHeaders} to avoid typing and casing mistakes.
	 *
	 * @return the headers of this response.
	 */
	public Map<String,String> headers();

	/** Returns the content of this response as a stream.
	 *
	 * @return the content of this response as a stream.
	 * @throws IOException
	 */
	public MeasurableInputStream contentAsStream() throws IOException;
}

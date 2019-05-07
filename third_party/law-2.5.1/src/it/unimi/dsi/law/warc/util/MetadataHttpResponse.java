package it.unimi.dsi.law.warc.util;

import java.net.URI;
import java.util.Map;

import org.apache.http.StatusLine;

/*
 * Copyright (C) 2012-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;

//RELEASE-STATUS: DIST

/** An abstract extention of {@link AbstractHttpResponse} which additionally provides support
 * for getting and setting metadata (i.e., {@link #uri()}, {@link #statusLine()}, {@link #status()} and {@link #headers()}). */

public abstract class MetadataHttpResponse extends AbstractHttpResponse {

	/** A special map used for headers: keys are case-insensitive, and multiple puts are converted into comma-separated values. */
	public static final class HeaderMap extends Object2ObjectLinkedOpenCustomHashMap<String, String> {
		private static final long serialVersionUID = 1L;

		public HeaderMap() {
			super(Util.CASE_INSENSITIVE_STRING_HASH_STRATEGY);
		}

		@Override
		public String put(String key, String value) {
			if (!containsKey(key)) return super.put(key, value);
			else return super.put(key, get(key) + "," + value);
		}
	}

	/** The URI that is currently contained in this response. */
	protected URI uri;
	/** The status line of this response. */
	protected StatusLine statusLine;
	/** The header map. */
	protected final Map<String,String> headerMap = new HeaderMap();

	@Override
	public URI uri() {
		return uri;
	}

	@Override
	public int status() {
		return statusLine.getStatusCode();
	}

	@Override
	public StatusLine statusLine() {
		return statusLine;
	}

	@Override
	public Map<String, String> headers() {
		return headerMap;
	}

	/** Sets the url.
	 *
	 * @param url the url.
	 */
	public void uri(URI url) {
		this.uri = url;
	}

	/** Sets the status line
	 *
	 * @param statusLine the status line.
	 */
	public void statusLine(StatusLine statusLine) {
		this.statusLine = statusLine;
	}

	/** Sets the headers.
	 *
	 * @param headerMap the content (it may be {@code null},
	 * in this case the headers will be left empty).
	 */
	public void headers(Object2ObjectMap<String, String> headerMap) {
		this.headerMap.clear();
		if (headerMap != null) this.headerMap.putAll(headerMap);
	}
}

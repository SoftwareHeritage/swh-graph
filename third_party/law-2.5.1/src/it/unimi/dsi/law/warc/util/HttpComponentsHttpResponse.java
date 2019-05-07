package it.unimi.dsi.law.warc.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;

import com.google.common.io.ByteStreams;

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

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.fastutil.objects.AbstractObject2ObjectMap;
import it.unimi.dsi.fastutil.objects.AbstractObjectIterator;
import it.unimi.dsi.fastutil.objects.AbstractObjectSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.law.warc.io.WarcRecord;

// RELEASE-STATUS: DIST

/** An concrete subclass of {@link AbstractHttpResponse} that implements
 * missing methods by wrapping an Apache HTTP Components {@link HttpResponse}.
 *
 * <p>A typical use case of this class is storing in a {@link WarcRecord} the
 * content of an Apache HTTP Components {@link HttpResponse}. The nested
 * class {@link HttpResponseHeaderMap} can be used in other classes to expose
 * as a standard Java map the header-related methods of an Apache HTTP Components {@link HttpResponse}.
 *
 * <p>To be able to return a {@link MeasurableInputStream}, this class caches the
 * result of the underlying Apache HTTP Components {@link HttpResponse}. The cache
 * is never shrunk, but {@link #clear()} will trim it to length zero. You can override
 * {@link #contentAsStream()} to provide different ways of perform the caching.
 */

public class HttpComponentsHttpResponse extends AbstractHttpResponse {

	/** A wrapper class exposing headers in {@link it.unimi.dsi.law.warc.util.HttpResponse#headers()}
	 * format by delegating to an {@link HttpResponse}. */
	@SuppressWarnings("serial")
	public final static class HttpResponseHeaderMap extends AbstractObject2ObjectMap<String, String> {
		private HttpResponse httpResponse;

		/** Sets the response whose headers will be wrapped by this map.
		 *
		 * @param httpResponse a response whose headers will be exposed by this map.
		 */
		public void response(final HttpResponse httpResponse) {
			this.httpResponse = httpResponse;
		}

		@Override
		public ObjectSet<it.unimi.dsi.fastutil.objects.Object2ObjectMap.Entry<String, String>> object2ObjectEntrySet() {
			return new AbstractObjectSet<Object2ObjectMap.Entry<String,String>>() {
				private final Header[] header = httpResponse.getAllHeaders();

				@Override
				public ObjectIterator<it.unimi.dsi.fastutil.objects.Object2ObjectMap.Entry<String, String>> iterator() {
					return new AbstractObjectIterator<Object2ObjectMap.Entry<String,String>>() {
						private int i = 0;
						@Override
						public boolean hasNext() {
							return i < header.length;
						}

						@Override
						public it.unimi.dsi.fastutil.objects.Object2ObjectMap.Entry<String, String> next() {
							if (! hasNext()) throw new NoSuchElementException();
							return new BasicEntry<String,String>(header[i].getName(), header[i++].getValue());
						}

					};
				}

				@Override
				public int size() {
					return header.length;
				}

			};
		}

		@Override
		public String get(Object key) {
			final Header[] header = httpResponse.getHeaders(key.toString());
			if (header == null || header.length == 0) return null;
			if (header.length == 1) return header[0].getValue();
			final StringBuilder stringBuilder = new StringBuilder();
			for(int i = 0; i < header.length; i++) {
				if (i != 0) stringBuilder.append(',');
				stringBuilder.append(header[i].getValue());
			}

			return stringBuilder.toString();
		}

		@Override
		public int size() {
			return httpResponse.getAllHeaders().length;
		}
	}

	/** The URL associated with {@link #httpResponse}. */
	protected URI url;
	/** The response wrapped by this {@link HttpComponentsHttpResponse}. */
	protected HttpResponse httpResponse;
	/** The header map wrapping {@link #httpResponse}'s headers. */
	protected HttpResponseHeaderMap headerMap = new HttpResponseHeaderMap();
	/** A cache for the  {@linkplain HttpEntity#getContent() content} of the {@linkplain org.apache.http.HttpResponse#getEntity() entity}
	 * returned by {@link #httpResponse}. */
	protected FastByteArrayOutputStream cachedContent = new FastByteArrayOutputStream();
	/** Whether the {@linkplain HttpEntity#getContent() content} of the {@linkplain org.apache.http.HttpResponse#getEntity() entity}
	 * returned by {@link #httpResponse} has been cached in {@link #cachedContent}. */
	protected boolean contentReady;

	/** Creates a new instance.
	 *
	 * <p>Use {@link #set(URI, HttpResponse)} to wrap an Apache HTTP Components {@link HttpResponse}.
	 */
	public HttpComponentsHttpResponse() {}

	/** Creates a new instance wrapping a given Apache HTTP Components {@link HttpResponse}.
	 *
	 * @param url the URL for <code>httpResponse</code>.
	 * @param httpResponse the response to be wrapped.
	 */
	public HttpComponentsHttpResponse(final URI url, final HttpResponse httpResponse) {
		set(url, httpResponse);
	}

	/** Sets the response wrapped by this instance.
	 *
	 * @param url the URL for <code>httpResponse</code>.
	 * @param httpResponse the response to be wrapped.
	 */
	public void set(final URI url, final HttpResponse httpResponse) {
		this.url = url;
		this.httpResponse = httpResponse;
		headerMap.response(httpResponse);
		contentReady = false;
		cachedContent.reset();
	}

	/** Invokes {@link EntityUtils#consume(HttpEntity)} on the entity returned by the underlying
	 * Apache HTTP Components {@link HttpResponse}. */
	public void consume() throws IOException {
		EntityUtils.consume(httpResponse.getEntity());
	}

	@Override
	public int status() {
		return httpResponse.getStatusLine().getStatusCode();
	}

	@Override
	public StatusLine statusLine() {
		return httpResponse.getStatusLine();
	}

	@Override
	public Map<String, String> headers() {
		return headerMap;
	}

	@Override
	public MeasurableInputStream contentAsStream() throws IOException {
	    final HttpEntity entity = httpResponse.getEntity();
	    if (entity == null) return null;
	    if (! contentReady) {
	    	final InputStream instream = entity.getContent();
	    	try {
	    		contentReady = true;
	    		ByteStreams.copy(entity.getContent(), cachedContent);
	    	} finally {
				try { instream.close(); } catch (Exception ignore) {}
            }
        }

		return new FastByteArrayInputStream(cachedContent.array, 0, cachedContent.length);
	}

	@Override
	public URI uri() {
		return url;
	}

	@Override
	public boolean fromWarcRecord(WarcRecord record) throws IOException {
		throw new UnsupportedOperationException();
	}

	/** Clears this {@link HttpComponentsHttpResponse}, in particular trimming the content cache. */
	public void clear() {
		httpResponse = null;
		headerMap.response(null);
		contentReady = false;
		cachedContent.reset();
		cachedContent.trim();
	}
}

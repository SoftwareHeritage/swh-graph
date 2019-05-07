package it.unimi.dsi.law.bubing.util;

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
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;

import it.unimi.dsi.lang.MutableString;

// RELEASE-STATUS: DIST

/** Static methods to manipulate normalized, canonical URLs.
 *
 *@deprecated Use <a href="http://law.di.unimi.it/software/bubing-docs/it/unimi/di/law/bubing/util/BURL.html">BUbiNG's BURL</a>.
 */
@Deprecated
public final class BURL {
	private static final Logger LOGGER = LoggerFactory.getLogger(BURL.class);

	private static final boolean DEBUG = false;

	/** Characters that will cause a URI spec to be rejected. */
	public static final char[] FORBIDDEN_CHARS = { '\n', '\r' };

	/** A list of bad characters. It includes the backslash, replaced by the slash, and illegal characters
	 * such as spaces and braces, which are replaced by the equivalent percent escape. Square brackets
	 * are percent-escaped, too, albeit legal in some circumstances, as they appear frequently in paths. */
	public static final char[] BAD_CHAR = new char[] { '\\', ' ', '\t', '[', ']', '"', '|', '{', '}', '^', '<', '>', '`' };
	/** Substitutes for {@linkplain #BAD_CHAR bad characters}. */
	public static final String[] BAD_CHAR_SUBSTITUTE = new String[BAD_CHAR.length];

	static {
		BAD_CHAR_SUBSTITUTE[0] = "/";
		for(int i = BAD_CHAR.length; i-- != 1;) BAD_CHAR_SUBSTITUTE[i] = (BAD_CHAR[i] < 16 ? "%0" : "%") + Integer.toHexString(BAD_CHAR[i]);
	}

	private BURL() {}

	/**  Creates a new BUbiNG URL from a string specification if possible, or returns {@code null} otherwise.
	 *
	 * @param spec the string specification for a URL.
	 * @return a BUbiNG URL corresponding to <code>spec</code> without possibly the fragment, or {@code null} if <code>spec</code> is malformed.
	 * @see #parse(MutableString)
	 */

	public static URI parse(final String spec) {
		return parse(new MutableString(spec));
	}

	/** Creates a new BUbiNG URL from a {@linkplain MutableString mutable string}
	 * specification if possible, or returns {@code null} otherwise.
	 *
	 * <p>The conditions for this method not returning {@code null} are as follows:
	 * <ul>
	 * <li><code>spec</code>, once trimmed, must not contain characters in {@link #FORBIDDEN_CHARS};
	 * <li>once characters in {@link #BAD_CHAR} have been substituted with the corresponding
	 * strings in {@link #BAD_CHAR_SUBSTITUTE}, and percent signs not followed by two hexadecimal
	 * digits have been substituted by <code>%25</code>, <code>spec</code> must not throw
	 * an exception when {@linkplain URI#URI(java.lang.String) made into a URI}.
	 * <li>the {@link URI} instance so obtained must not be {@linkplain URI#isOpaque() opaque}.
	 * <li>the {@link URI} instance so obtained, if {@linkplain URI#isAbsolute() absolute},
	 * must have a non-{@code null} {@linkplain URI#getAuthority() authority}.
	 * </ul>
	 *
	 * <p>For efficiency, this method modifies the provided specification,
	 * and in particular it makes it {@linkplain MutableString#loose() loose}. <i>Caveat emptor</i>.
	 *
	 * <p>Fragments are removed (for a web crawler fragments are just noise). {@linkplain URI#normalize() Normalization}
	 * is applied for you. Scheme and host name are downcased. If the URL has no host name, it is guaranteed
	 * that the path is non-{@code null} and non-empty (by adding a slash, if necessary). If
	 * the host name ends with a dot, it is removed.
	 *
	 * @param spec the string specification for a URL; <strong>it can be modified by this method</strong>, and
	 * in particularly it will always be made {@linkplain MutableString#loose() loose}.
	 * @return a BUbiNG URL corresponding to <code>spec</code> without possibly the fragment, or {@code null} if
	 * <code>spec</code> is malformed.
	 * @see #parse(String)
	 */

	public static URI parse(final MutableString spec) {

		if (DEBUG) LOGGER.debug("parse(" + spec + ")");
		spec.loose().trim();
		if (spec.indexOfAnyOf(FORBIDDEN_CHARS) != -1) return null;

		// By the book, but flexible.
		spec.replace(BAD_CHAR, BAD_CHAR_SUBSTITUTE);

		// Find percents not followed by two hexadecimal digits and fix them.
		final char[] a = spec.array();
		final int l = spec.length();

		for(int i = l; i-- != 0;) {
			if (a[i] == '%' && (i >= l - 2 || ! isHexDigit(a[i + 1]) || ! isHexDigit(a[i + 2]))) spec.insert(i + 1, "25");
		}

		try {
			final URI uri = new URI(spec.toString()).normalize();

			if (uri.isOpaque()) return null;

			// Let us force parsing host, user info and port, or get an exception otherwise.
			if (uri.isAbsolute()) uri.parseServerAuthority();

			// Downcase
			String scheme = uri.getScheme();
			if (scheme != null) {
				if (scheme.indexOf('\0') != -1) return null; // Workaround for URI bug
				scheme = scheme.toLowerCase();
			}

			// No absolute URL without authority (e.g., file://).
			if (uri.isAbsolute() && uri.getAuthority() == null) return null;

			// Workaround for URI bug
			if (uri.getPath() != null && uri.getPath().indexOf('\0') != -1) return null;
			if (uri.getUserInfo() != null && uri.getUserInfo().indexOf('\0') != -1) return null;
			if (uri.getQuery() != null && uri.getQuery().indexOf('\0') != -1) return null;

			// Remove trailing dot in host name if present and downcase
			String host = uri.getHost();
			if (host != null) {
				if (host.indexOf('\0') != -1) return null;  // Workaround for URI bug
				if (host.endsWith(".")) host = host.substring(0, host.length() - 1);
				host = host.toLowerCase();
			}

			// Substitute empty path with slash in absolute URIs.
			String rawPath = uri.getRawPath();

			if (host != null && (rawPath == null || rawPath.length() == 0)) rawPath = "/";

			// Rebuild, discarding fragment, parsing again a purely ASCII string and renormalizing (convoluted, but it does work).
			return new URI(sanitizeAndRepack(scheme, uri.getRawUserInfo(), host, uri.getPort(), rawPath, uri.getRawQuery())).normalize();
		}
		catch (final URISyntaxException e) {
			return null;
		}
		catch(final Exception e) {
			LOGGER.warn("Unexpected exception while parsing " + spec, e);
			return null;
		}
	}

	/** If the argument string does not contain non-ASCII characters, returns the string itself;
	 * otherwise, encodes non-ASCII characters by %XX-encoded UTF-8 sequences.
	 *
	 * @param s a string.
	 * @return <code>c</code> with non-ASCII characters replaced by %XX-encoded UTF-8 sequences.
	 */
	private static String sanitize(final String s) {
    	int i = s.length();
        for(i = s.length(); i-- != 0;) if (s.charAt(i) >= (char)128) break;
        if (i == -1) return s;

		final ByteBuffer byteBuffer = Charsets.UTF_8.encode(CharBuffer.wrap(s));
		final StringBuilder stringBuilder = new StringBuilder();

		while (byteBuffer.hasRemaining()) {
			final int b = byteBuffer.get() & 0xff;
			if (b >= 0x80) stringBuilder.append('%').append(Integer.toHexString(b >> 4 & 0xf)).append(Integer.toHexString(b & 0xf));
			else stringBuilder.append((char)b);
		}

		return stringBuilder.toString();
	}

	/** {@linkplain #sanitize(String) Sanitizes} all arguments (any of which may be {@code null})
	 * and repack them as the string representation of a URI. The behaviour of this method is
	 * similar to that of {@link URI#URI(String, String, String, int, String, String, String)}, but
	 * we do not escape reserved characters, and the result is guaranteed to be an ASCII string.
	 *
	 * @return the string representation of a URI formed by the given components with non-ASCII
	 * characters replaced by %XX-encoded UTF-8 sequences.
	 * @see #sanitize(String)
	 */
	private static String sanitizeAndRepack(final String scheme, final String userInfo, final String host, final int port, final String path, final String query) {
		final StringBuffer sb = new StringBuffer();
		if (scheme != null) sb.append(sanitize(scheme)).append(':');
		if (host != null) {
			sb.append("//");
			if (userInfo != null) sb.append(sanitize(userInfo)).append('@');
			final boolean needBrackets = host.indexOf(':') >= 0 && ! host.startsWith("[") && ! host.endsWith("]");
			if (needBrackets) sb.append('[');
			sb.append(sanitize(host));
			if (needBrackets) sb.append(']');
			if (port != -1) sb.append(':').append(port);
		}
		if (path != null) sb.append(sanitize(path));
		if (query != null) sb.append('?').append(sanitize(query));
		return sb.toString();
	}

	/** Creates a new BUbiNG URL from a normalized ASCII string represented by a byte array.
	 *
	 * <p>The string represented by the argument will <em>not</em> go through {@link #parse(MutableString)}.
	 * {@link URI#create(String)} will be used instead.
	 *
	 * @param normalized a normalized URI string represented by a byte array.
	 * @return the corresponding BUbiNG URL.
	 * @throws IllegalArgumentException if <code>normalized</code> does not parse correctly.
	 */
	public static URI fromNormalizedByteArray(final byte[] normalized) {
		return URI.create(toString(normalized));
	}

	/** Creates a new BUbiNG URL from a normalized ASCII string representing scheme and
	 * authority and a byte-array representation of a normalized ASCII path and query.
	 *
	 * <p>This method is intended to combine the results of {@link #schemeAndAuthority(URI)}/
	 * {@link #schemeAndAuthority(byte[])} and {@link #pathAndQueryAsByteArray(byte[])}(
	 * {@link #pathAndQueryAsByteArray(URI)}.
	 *
	 * @param schemeAuthority an ASCII string representing scheme and authorty.
	 * @param normalizedPathQuery the byte-array representation of a normalized ASCII path and query.
	 * @return the corresponding BUbiNG URL.
	 * @throws IllegalArgumentException if the two parts, concatenated, do not parse correctly.
	 */
	public static URI fromNormalizedSchemeAuthorityAndPathQuery(final String schemeAuthority, final byte[] normalizedPathQuery) {
		final char[] array = new char[schemeAuthority.length() + normalizedPathQuery.length];
		schemeAuthority.getChars(0, schemeAuthority.length(), array, 0);
		for(int i = array.length, j = normalizedPathQuery.length; j-- != 0;) array[--i] = (char)normalizedPathQuery[j];
		return URI.create(new String(array));
	}

	private static boolean isHexDigit(final char c) {
		return c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f';
	}

	/** Returns an ASCII byte-array representation of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return an ASCII byte-array representation of <code>uri</code>
	 */
	public static byte[] toByteArray(final URI url) {
		final String s = url.toString();
		final byte[] result = new byte[s.length()];
		for (int i = result.length; i-- != 0;) {
			assert s.charAt(i) < (char)0x80 : s.charAt(i);
			result[i] = (byte)(s.charAt(i) & 0x7F);
		}
		return result;
	}

	/** Returns an ASCII byte-array representation of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return an ASCII byte-array representation of <code>uri</code>
	 */
	public static String toString(final byte[] url) {
		final char[] array = new char[url.length];
		// This needs to be fast.
		for(int i = array.length; i-- != 0;) {
			assert url[i] < (char)0x80 : url[i];
			array[i] = (char)url[i];
		}
		return new String(array);
	}

	/** Returns an ASCII byte-array representation of
	 * the {@linkplain URI#getRawPath() raw path} and {@linkplain URI#getRawQuery() raw query} of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return an ASCII byte-array representation of
	 * the {@linkplain URI#getRawPath() raw path} and {@linkplain URI#getRawQuery() raw query} of a BUbiNG URL.
	 */
	public static byte[] pathAndQueryAsByteArray(final URI url) {
		final String query = url.getRawQuery();
		final String path = url.getRawPath();
		final byte[] result = new byte[path.length() + (query != null ? 1 + query.length() : 0)];

		for (int i = path.length(); i-- != 0;) {
			assert path.charAt(i) < (char)0x80 : path.charAt(i);
			result[i] = (byte)(path.charAt(i) & 0x7F);
		}

		if (query != null) {
			result[path.length()] = '?';
			for (int j = query.length(), i = result.length; j-- != 0;) {
				assert query.charAt(j) < (char)0x80 : query.charAt(j);
				result[--i] = (byte)(query.charAt(j) & 0x7F);
			}
		}

		return result;
	}

	/** Returns the concatenated {@linkplain URI#getRawPath() raw path} and {@linkplain URI#getRawQuery() raw query} of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return the concatenated {@linkplain URI#getRawPath() raw path} and {@linkplain URI#getRawQuery() raw query} of <code>uri</code>.
	 */
	public static String pathAndQuery(final URI url) {
		final String query = url.getRawQuery();
		return query != null ? url.getRawPath() + '?' + query : url.getRawPath();
	}

	/** Returns the concatenated {@linkplain URI#getScheme()} and {@link URI#getRawAuthority() raw authority} of a BUbiNG URL.
	 *
	 * @param url a BUbiNG URL.
	 * @return the concatenated {@linkplain URI#getScheme()} and {@link URI#getRawAuthority() raw authority} of <code>uri</code>.
	 */
	public static String schemeAndAuthority(final URI url) {
		return url.getScheme() + "://" + url.getRawAuthority();
	}

	private final static byte[] DOUBLE_BAR = new byte[] { '/', '/' };

	/** Extracts the host of an absolute BUbiNG URL in its byte-array representation.
	 *
	 * @param url a byte-array representation of a BUbiNG URL.
	 * @return the host of <code>url</code>.
	 */
	public static String host(final byte[] url){
		int startHost = Bytes.indexOf(url, DOUBLE_BAR) + DOUBLE_BAR.length;
		final int endAuthority = ArrayUtils.indexOf(url, (byte)'/', startHost);
		final int atPosition = ArrayUtils.indexOf(url, (byte)'@', startHost);
		if (atPosition != -1 && atPosition < endAuthority) startHost = atPosition + 1;
		final int colonPosition = ArrayUtils.indexOf(url, (byte)':', startHost);
		final int endHost = colonPosition != -1 && colonPosition < endAuthority ? colonPosition : endAuthority;
		final char[] array = new char[endHost - startHost];
		for(int i = endHost - startHost; i-- != 0;) array[i] = (char)url[i + startHost];
		return new String(array);
	}

	/** Extracts the path and query of an absolute BUbiNG URL in its byte-array representation.
	 *
	 * @param url a byte-array representation of a BUbiNG URL.
	 * @return the path and query in byte-array representation.
	 */
	public static byte[] pathAndQueryAsByteArray(final byte[] url){
		final int startAuthority = Bytes.indexOf(url, DOUBLE_BAR) + DOUBLE_BAR.length;
		return Arrays.copyOfRange(url, ArrayUtils.indexOf(url, (byte)'/', startAuthority), url.length);
	}

	/** Extracts the host part from a scheme and authority by removing the scheme, the user info and the port number.
	 *
	 * @param schemeAuthority a scheme and authority.
	 * @return the host part.
	 */
	public static String hostFromSchemeAndAuthority(final String schemeAuthority){
		final int startOfAuthority = schemeAuthority.indexOf(":") + 3;
		final int atPosition = schemeAuthority.indexOf('@', startOfAuthority);
		final int startOfHost = atPosition != -1  ? atPosition + 1 : startOfAuthority;
		final int colonPosition = schemeAuthority.indexOf(':', startOfHost);
		return colonPosition == -1 ? schemeAuthority.substring(startOfHost) : schemeAuthority.substring(startOfHost, colonPosition);
	}

	/** Extracts the scheme and authority of an absolute BUbiNG URL in its byte-array representation.
	 *
	 * @param url an absolute BUbiNG URL.
	 * @return the scheme and authority of <code>url</code>.
	 */
	public static String schemeAndAuthority(final byte[] url){
		int i, j;
		for(i = 0, j = 2; ; i++) if (url[i] == '/' && j-- == 0) break;
		final char[] array = new char[i];
		for(i = array.length; i-- != 0;) array[i] = (char)url[i];
		return new String(array);
	}

	/** Returns the memory usage associated to a byte array.
	 *
	 * <p>This method is useful in establishing the memory footprint of URLs in byte-array representation.
	 *
	 * @param array a byte array.
	 * @return its memory usage in bytes.
	 */
	public static int memoryUsageOf(final byte[] array) {
		return ((16 + array.length + 7) & -1 << 3) + // Obtained by Classmexer on a 64-bit Sun JVM for Intel.
				8; // This accounts for the space used by the FIFO queue.
	}
}

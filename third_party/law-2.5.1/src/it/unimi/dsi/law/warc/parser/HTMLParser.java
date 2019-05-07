package it.unimi.dsi.law.warc.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.law.bubing.util.BURL;
import it.unimi.dsi.law.warc.io.InspectableBufferedInputStream;
import it.unimi.dsi.law.warc.util.ByteArrayCharSequence;
import it.unimi.dsi.law.warc.util.HttpResponse;
import it.unimi.dsi.law.warc.util.Response;
import it.unimi.dsi.law.warc.util.Util;
import it.unimi.dsi.util.TextPattern;
import net.htmlparser.jericho.CharacterReference;
import net.htmlparser.jericho.EndTag;
import net.htmlparser.jericho.EndTagType;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.HTMLElements;
import net.htmlparser.jericho.Segment;
import net.htmlparser.jericho.StartTag;
import net.htmlparser.jericho.StartTagType;
import net.htmlparser.jericho.StreamedSource;

// RELEASE-STATUS: DIST

/** An HTML parser with additional responsibilities (such as guessing the character encoding
 * and resolving relative URLs).
 *
 * <p>An instance of this class contains buffers and classes that makes it possible to
 * parse quickly a {@link it.unimi.dsi.law.warc.util.HttpResponse}. Instances are heavyweight&mdash;they
 * should be pooled and shared, since their usage is transitory and CPU-intensive.
 *
 */

public class HTMLParser implements Parser {
	private final static Logger LOGGER = LoggerFactory.getLogger(HTMLParser.class);

	public final static class SetLinkReceiver implements LinkReceiver {
		private final Set<URI> urls = new ObjectLinkedOpenHashSet<>();

		@Override
		public void location(URI location) {
			urls.add(location);
		}

		@Override
		public void metaLocation(URI location) {
			urls.add(location);
		}

		@Override
		public void metaRefresh(URI refresh) {
			urls.add(refresh);
		}

		@Override
		public void link(URI link) {
			urls.add(link);
		}

		@Override
		public void init(URI responseUrl) {
			urls.clear();
		}

		@Override
		public Iterator<URI> iterator() {
			return urls.iterator();
		}
	}

	/** A class computing the digest of a page.
	 *
	 * <p>The page is somewhat simplified before being passed (as a sequence of bytes obtained
	 * by breaking each character into the upper and lower byte) to {@link MessageDigest#update(byte[])}.
	 * All start/end tags are case-normalized, and all their content (except for the
	 * element-type name) is removed. An exception is made for <code>SRC</code> attribute of
	 * <code>FRAME</code> and <code>IFRAME</code> elements, as they are necessary to
	 * distinguish correctly framed pages without alternative text. The attributes will be resolved
	 * w.r.t. the {@linkplain #uri(URI) URL associated to the page}.
	 *
	 * <p>To avoid clashes between digests coming from different sites, you can optionally set a URL
	 * whose authority that will be used to update the digest before adding the actual text page.
	 * You can set the URL with {@link #uri(URI)}. A good idea is to use
	 * the host name (or even the authority).
	 */


	private final static class DigestAppendable implements Appendable {
		/** The size of the internal buffer. */
		private final static int BYTE_BUFFER_SIZE = 1024;

		/** Cached byte representations of all opening tags. The map must be queried using {@linkplain HTMLElementName Jericho names}. */
		private static final Reference2ObjectOpenHashMap<String, byte[]> startTags;

		/** Cached byte representations of all closing tags. The map must be queried using {@linkplain HTMLElementName Jericho names}. */
		private static final Reference2ObjectOpenHashMap<String, byte[]> endTags;

		static {
			final List<String> elementNames = HTMLElements.getElementNames();
			startTags = new Reference2ObjectOpenHashMap<String, byte[]>(elementNames.size());
			endTags = new Reference2ObjectOpenHashMap<String, byte[]>(elementNames.size());

			// Set up defaults for bizarre element types
			startTags.defaultReturnValue(Util.getASCIIBytes("<unknown>"));
			endTags.defaultReturnValue(Util.getASCIIBytes("</unknown>"));

			// Scan all known element types and fill startTag/endTag
			for (String name : elementNames) {
				startTags.put(name, Util.getASCIIBytes("<" + name + ">"));
				endTags.put(name, Util.getASCIIBytes("</" + name + ">"));
			}
		}

		/** An internal buffer where bytes are accumulated to avoid excessive calls to
		 * {@link MessageDigest#update(byte)}. */
		private final byte byteBuffer[] = new byte[BYTE_BUFFER_SIZE];

		/** The algorithm used to compute the digest. */
		private final MessageDigest digester;

		/** The current number of bytes in {@link #byteBuffer}. */
		private int fill;

		/** Create a digest appendable using a given algorithm.
		 *
		 * @param digester a digesting algorithm. */
		public DigestAppendable(final MessageDigest digester) {
			this.digester = digester;
		}

		/** Initializes the digest computation.
		 *
		 * @param url a URL, or {@code null} for no URL.
		 */
		public void init(final URI url) {
			digester.reset();
			fill = 0;

			if (url != null) {
				append(url.getAuthority());
				append('\0');
			}
		}

		@Override
		public Appendable append(CharSequence csq, int start, int end) {
			for (int i = start; i < end; i++) {
				final char c = csq.charAt(i);
				if (fill >= BYTE_BUFFER_SIZE - 1) {
					digester.update(byteBuffer, 0, fill);
					fill = 0;
				}
				byteBuffer[fill++] = (byte)(c >> 8);
				byteBuffer[fill++] = (byte)c;
			}
			return this;
		}

		@Override
		public Appendable append(char c) {
			if (fill >= BYTE_BUFFER_SIZE - 1) {
				digester.update(byteBuffer, 0, fill);
				fill = 0;
			}
			byteBuffer[fill++] = (byte)(c >> 8);
			byteBuffer[fill++] = (byte)c;
			return this;
		}

		@Override
		public Appendable append(CharSequence csq) {
			return append(csq, 0, csq.length());
		}

		public byte[] digest() {
			digester.update(byteBuffer, 0, fill);
			fill = 0;
			return digester.digest();
		}

		private void update(byte[] a) {
			for (byte b : a) {
				if (fill == BYTE_BUFFER_SIZE) {
					digester.update(byteBuffer);
					fill = 0;
				}
				byteBuffer[fill++] = b;
			}
		}

		public void startTag(final StartTag startTag) {
			final String name = startTag.getName();
			update(startTags.get(name));

			// IFRAME or FRAME + SRC
			if (name == HTMLElementName.IFRAME || name == HTMLElementName.FRAME) {
				String s = startTag.getAttributeValue("src");
				if (s != null) {
					append('\"');
					append(s);
					append('\"');
				}
			}
		}

		public void endTag(final EndTag endTag) {
			update(endTags.get(endTag.getName()));
		}
	}

	/** The pattern prefixing the URL in a <code>META </code> <code>HTTP-EQUIV </code> element of refresh type. */
	private static final TextPattern URLEQUAL_PATTERN = new TextPattern("URL=", TextPattern.CASE_INSENSITIVE);
	/** The size of the internal Jericho buffer. */
	public static final int CHAR_BUFFER_SIZE = 65536;

	/** The character buffer. It is set up at construction time, but it can be changed later. */
	public final char[] buffer;
	/** The charset we guessed for the last response. */
	private String guessedCharset;
	/** The digesting algorithm used, or {@code null} if no digesting is to be performed. */
	private MessageDigest messageDigest;
	/** An object emboding the digest logic, or {@code null} for no digest computation. */
	private final DigestAppendable digestAppendable;
	/** The location URL from headers of the last response, if any, or {@code null}. */
	private URI location;
	/** The location URL from <code>META</code> elements of the last response, if any, or {@code null}. */
	private URI metaLocation;

	/**
	 * Builds a parser for link extraction and, possibly, digesting a page.
	 *
	 * @param messageDigest the digesting algorithm, or {@code null} if digesting will be performed.
	 */
	public HTMLParser(final MessageDigest messageDigest) {
		buffer = new char[CHAR_BUFFER_SIZE];
		this.messageDigest = messageDigest;
		digestAppendable = messageDigest == null ? null : new DigestAppendable(messageDigest);
	}


	/**
	 * Builds a parser for link extraction and, possibly, digesting a page.
	 *
	 * @param messageDigest the digesting algorithm (as a string).
	 * @throws NoSuchAlgorithmException
	 */
	public HTMLParser(final String messageDigest) throws NoSuchAlgorithmException {
		this(MessageDigest.getInstance(messageDigest));
	}


	/**
	 * Builds a parser for link extraction.
	 */
	public HTMLParser() {
		this((MessageDigest)null);
	}


	private void process(final LinkReceiver linkReceiver, final URI base, final String s) {
		if (s == null) return;
		URI url = BURL.parse(s);
		if (url == null) return;
		linkReceiver.link(base.resolve(url));
	}

	@Override
	public byte[] parse(final Response response, final LinkReceiver linkReceiver) throws IOException {
		final URI responseUrl = response.uri();
		final HttpResponse httpResponse = (HttpResponse)response;

		guessedCharset = "ISO-8859-1";

		// Try to guess using headers
		final String contentTypeHeader = httpResponse.headers().get(HttpHeaders.CONTENT_TYPE);
		if (contentTypeHeader != null) {
			final String headerCharset = getCharsetNameFromHeader(contentTypeHeader);
			if (headerCharset != null) guessedCharset = headerCharset;
		}

		final InputStream contentStream = httpResponse.contentAsStream();
		if (contentStream instanceof InspectableBufferedInputStream) {
			final InspectableBufferedInputStream inspectableStream = (InspectableBufferedInputStream)contentStream;
			final String metaCharset = getCharsetName(inspectableStream.buffer, inspectableStream.inspectable);
			if (metaCharset != null) guessedCharset = metaCharset;
		}

		LOGGER.debug("Guessing charset " + guessedCharset + " for URL " + responseUrl);

		Charset charset = Charsets.ISO_8859_1; // Fallback
		try {
			charset = Charset.forName(guessedCharset);
		}
		catch(IllegalCharsetNameException e) {
			LOGGER.warn("Response for " + responseUrl + " contained an illegal charset name: " + guessedCharset);
		}
		catch(UnsupportedCharsetException e) {
			LOGGER.warn("Response for " + responseUrl + " contained an unsupported charset: " + guessedCharset);
		}

		linkReceiver.init(responseUrl);

		// Get location if present
		location = null;
		metaLocation = null;

		if (httpResponse.headers().get(HttpHeaders.LOCATION) != null) {
			final URI location = BURL.parse(httpResponse.headers().get(HttpHeaders.LOCATION));
			if (location != null) {
				// This shouldn't happen by standard, but people unfortunately does it.
				if (! location.isAbsolute()) LOGGER.warn("Found relative header location URL: \"" + location + "\"");
				linkReceiver.location(this.location = responseUrl.resolve(location));
			}
		}

		@SuppressWarnings("resource")
		final StreamedSource streamedSource = new StreamedSource(new InputStreamReader(contentStream, charset));
		streamedSource.setBuffer(buffer);
		if (digestAppendable != null) digestAppendable.init(responseUrl);
		URI base = responseUrl;

		int lastSegmentEnd = 0;
		int inSpecialText = 0;
		for (Segment segment : streamedSource) {
			if (segment.getEnd() > lastSegmentEnd) {
				lastSegmentEnd = segment.getEnd();
				if (segment instanceof StartTag) {
					final StartTag startTag = (StartTag)segment;
					if (startTag.getTagType() != StartTagType.NORMAL) continue;
					final String name = startTag.getName();
					if (name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT) inSpecialText++;

					if (digestAppendable != null) digestAppendable.startTag(startTag);
					if (linkReceiver == null) continue; // No link receiver, nothing to do.

					// IFRAME or FRAME + SRC
					if (name == HTMLElementName.IFRAME || name == HTMLElementName.FRAME || name == HTMLElementName.EMBED) process(linkReceiver, base, startTag.getAttributeValue("src"));
					else if (name == HTMLElementName.IMG || name == HTMLElementName.SCRIPT) process(linkReceiver, base, startTag.getAttributeValue("src"));
					else if (name == HTMLElementName.OBJECT) process(linkReceiver, base, startTag.getAttributeValue("data"));
					else if (name == HTMLElementName.A || name == HTMLElementName.AREA || name == HTMLElementName.LINK) process(linkReceiver, base, startTag.getAttributeValue("href"));
					else if (name == HTMLElementName.BASE) {
						String s = startTag.getAttributeValue("href");
						if (s != null) {
							final URI uri = BURL.parse(s);
							if (uri != null) {
								if (uri.isAbsolute()) base = uri;
								else LOGGER.warn("Found relative BASE URL: \""+ uri + "\"");
							}
						}
					}

					// META REFRESH/LOCATION
					else if (name == HTMLElementName.META) {
						final String equiv = startTag.getAttributeValue("http-equiv");
						final String content = startTag.getAttributeValue("content");
						if (equiv != null && content != null) {
							equiv.toLowerCase();

							// http-equiv="refresh" content="0;URL=http://foo.bar/..."
							if (equiv.equals("refresh")) {

								final int pos = URLEQUAL_PATTERN.search(content);
								if (pos != -1) {
									final String urlPattern = content.substring(pos + URLEQUAL_PATTERN.length());
									final URI refresh = BURL.parse(urlPattern);
									if (refresh != null) {
										// This shouldn't happen by standard, but people unfortunately does it.
										if (! refresh.isAbsolute()) LOGGER.warn("Found relative META refresh URL: \"" + urlPattern + "\"");
										linkReceiver.metaRefresh(base.resolve(refresh));
									}
								}
							}

							// http-equiv="location" content="http://foo.bar/..."
							if (equiv.equals("location")) {
								final URI metaLocation = BURL.parse(content);
								if (metaLocation != null) {
									// This shouldn't happen by standard, but people unfortunately does it.
									if (! metaLocation.isAbsolute()) LOGGER.warn("Found relative META location URL: \"" + content + "\"");
									linkReceiver.metaLocation(this.metaLocation = base.resolve(metaLocation));
								}
							}
						}
					}
				}
				else if (segment instanceof EndTag) {
					final EndTag endTag = (EndTag)segment;
					final String name = endTag.getName();
					if (name == HTMLElementName.STYLE || name == HTMLElementName.SCRIPT) inSpecialText--;

					if (digestAppendable != null) {
						if (endTag.getTagType() != EndTagType.NORMAL) continue;
						digestAppendable.endTag(endTag);
					}
				}
				else if (digestAppendable != null && inSpecialText == 0) {
					if (segment instanceof CharacterReference) ((CharacterReference)segment).appendCharTo(digestAppendable);
					else digestAppendable.append(segment);
				}
			}
		}

		return digestAppendable != null ? digestAppendable.digest() : null;
	}

	public String guessedCharset() {
		return guessedCharset;
	}

	/** Returns the BURL location header, if present; if it is not present, but the page contains a valid metalocation, the latter
	 *  is returned. Otherwise, {@code null} is returned.
	 *
	 * @return the location (or metalocation), if present; {@code null} otherwise.
	 */
	public URI location() {
		//TODO: see if we must derelativize
		if (location != null) return location;
		else if (metaLocation != null) return metaLocation;
		else return null;
	}

	/** Used by {@link #getCharsetName(byte[], int)}. */
	private static final TextPattern META_PATTERN = new TextPattern("<meta", TextPattern.CASE_INSENSITIVE);
	/** Used by {@link #getCharsetName(byte[], int)}. */
	private static final Pattern HTTP_EQUIV_PATTERN = Pattern.compile(".*http-equiv\\s*=\\s*('|\")?content-type('|\")?.*", Pattern.CASE_INSENSITIVE);
	/** Used by {@link #getCharsetName(byte[], int)}. */
	private static final Pattern CONTENT_PATTERN = Pattern.compile(".*content\\s*=\\s*('|\")([^'\"]*)('|\").*", Pattern.CASE_INSENSITIVE);
	/** Used by {@link #getCharsetName(byte[], int)}. */
	private static final Pattern CHARSET_PATTERN = Pattern.compile (".*charset\\s*=\\s*(([\\041-\\0176&&[^<>\\{\\}\\\\/:,;@?=]])+|\"[^\"]*\").*", Pattern.CASE_INSENSITIVE);

	/** Returns the charset name as indicated by a <code>META</code>
	 * <code>HTTP-EQUIV</code> element, if
	 * present, interpreting the provided byte array as a sequence of
	 * ISO-8859-1-encoded characters. Only the first such occurrence is considered (even if
	 * it might not correspond to a valid or available charset).
	 *
	 * <p><strong>Beware</strong>: it might not work if the
	 * <em>value</em> of some attribute in a <code>meta</code> tag
	 * contains a string matching (case insensitively) the r.e.
	 * <code>http-equiv\s*=\s*('|")content-type('|")</code>, or
	 * <code>content\s*=\s*('|")[^"']*('|")</code>.
	 *
	 * @param buffer a buffer containing raw bytes that will be interpreted as ISO-8859-1 characters.
	 * @param length the number of significant bytes in the buffer.
	 * @return the charset name, or {@code null} if no
	 * charset is specified; note that the charset might be not valid or not available.
	 */

	public static String getCharsetName(final byte buffer[], final int length) {
		int start = 0;
		while((start = META_PATTERN.search(buffer, start, length)) != -1) {

			/* Look for attribute http-equiv with value content-type,
			 * if present, look for attribute content and, if present,
			 * return its value. */

			int end = start;
			while(end < length && buffer[end] != '>') end++; // Look for closing '>'
			if (end == length) return null; // No closing '>'

			final ByteArrayCharSequence tagContent = new ByteArrayCharSequence(buffer, start + META_PATTERN.length(), end - start - META_PATTERN.length());
			if (HTTP_EQUIV_PATTERN.matcher(tagContent).matches()) {
				final Matcher m = CONTENT_PATTERN.matcher(tagContent);
				if (m.matches()) return getCharsetNameFromHeader(m.group(2)); // got it!
			}

			start = end + 1;
		}

		return null; // no '<meta' found
	}

	/** Extracts the charset name from the header value of a <code>content-type</code>
	 *  header.
	 *
	 *  TODO: explain better
	 *  <strong>Warning</strong>: it might not work if someone puts the string <code>charset=</code>
	 *  in a string inside some attribute/value pair.
	 *
	 *  @param headerValue The value of a <code>content-type</code> header.
	 *  @return the charset name, or {@code null} if no
	 *  charset is specified; note that the charset might be not valid or not available.
	 */
	public static String getCharsetNameFromHeader(final String headerValue) {
		final Matcher m = CHARSET_PATTERN.matcher(headerValue);
		if (m.matches()) {
			final String s = m.group(1);
			int start = 0, end = s.length();
			// TODO: we discard delimiting single/double quotes; is it necessary?
			if (end > 0 && (s.charAt(0) == '\"' || s.charAt(0) == '\'')) start = 1;
			if (end > 0 && (s.charAt(end - 1) == '\"' || s.charAt(end - 1) == '\'')) end--;
			if (start < end) return s.substring(start, end);
		}
		return null;
	}

	@Override
	public boolean apply(final Response response) {
		if (! (response instanceof HttpResponse)) return false;
		HttpResponse httpResponse = (HttpResponse)response;
		final String contentType = httpResponse.headers().get(HttpHeaders.CONTENT_TYPE);
		return contentType != null && contentType.startsWith("text/");
	}

	@Override
	public Object clone() {
		return new HTMLParser(messageDigest);
	}

}

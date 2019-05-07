package it.unimi.dsi.law.warc.parser;

import java.lang.reflect.Field;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.law.warc.util.Util;
import it.unimi.dsi.parser.Attribute;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.Element;
import it.unimi.dsi.parser.callback.Callback;

// RELEASE-STATUS: DIST

/** A callback computing the digest of a page.
 *
 * <p>The page is somewhat simplified before being passed (as a sequence of bytes obtained
 * by breaking each character into the upper and lower byte) to {@link MessageDigest#update(byte[])}.
 * All start/end tags are case-normalized, and all their content (except for the
 * element-type name) is removed. An exception is made for <code>SRC</code> attribute of
 * <code>FRAME</code> and <code>IFRAME</code> elements, as they are necessary to
 * distinguish correctly framed pages without alternative text. The attributes will be resolved
 * w.r.t. the {@linkplain #url(URI) URL associated to the page}.
 *
 * <p>To avoid clashes between digests coming from different sites, you can optionally set a URL
 * whose authority that will be used to update the digest before adding the actual text page.
 * You can set the URL with {@link #url(URI)}. A good idea is to use
 * the host name (or even the authority).
 */

public class Digester implements Callback {
	/** The size of the internal buffer. */
	private final static int BYTE_BUFFER_SIZE = 8 * 1024;
	/** A char array used to separate the {@link #url} from the content of the page. */
	private final static char AUTHORITY_DELIMITER[] = "\u0000".toCharArray();
	/** A char array used to delimit attribute values. */
	private final static char ATTRIBUTE_VALUE_DELIMITER[] = "\"".toCharArray();

	private static final boolean DEBUG = false;

	/** Cached byte representations of all opening tags. */
	private static final Object2ObjectOpenHashMap<Element,byte[]> startTag;
	/** Cached byte representations of all closing tags. */
	private static final Object2ObjectOpenHashMap<Element,byte[]> endTag;

	/** A resuable message digester. */
	private final MessageDigest md;
	/** An internal buffer where bytes are accumulated to avoid excessive calls to {@link MessageDigest#update(byte)}. */
	private byte byteBuffer[] = new byte[BYTE_BUFFER_SIZE];
	/** The current number of bytes in {@link #byteBuffer}. */
	private int fill;
	/** The URI for the next digest, set by {@link #url(URI)}. */
	private URI url;
	/** The digest of the last page we parsed. */
	private byte[] digest;

	static {
		startTag = new Object2ObjectOpenHashMap<Element,byte[]>();
		endTag = new Object2ObjectOpenHashMap<Element,byte[]>();

		// Scan all known element types and fill startTag/endTag
		for(Field f: Element.class.getFields()) {
			if (f.getType() == Element.class) {
				Element element;
				try {
					element = (Element)f.get(null);
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
				startTag.put(element, Util.getASCIIBytes("<" + element + ">"));
				endTag.put(element, Util.getASCIIBytes("</" + element + ">"));
			}
		}

		// Set up defaults for bizarre element types
		startTag.defaultReturnValue(Util.getASCIIBytes("<unknown>"));
		endTag.defaultReturnValue(Util.getASCIIBytes("</unknown>"));
	}


	/** Creates a new callback using the given message digest.
	 *
	 * @param algorithm a message digest algorithm (to be passed to {@link MessageDigest#getInstance(java.lang.String)}).
	 */

	public Digester(String algorithm) throws NoSuchAlgorithmException {
		this.md = MessageDigest.getInstance(algorithm);
	}


	public void configure(BulletParser parser) {
		parser.parseTags(true);
		parser.parseAttributes(true);
		parser.parseText(true);
		parser.parseAttribute(Attribute.SRC);
	}

	/** Updates the digest with the given array.
	 *
	 * @param c the array from which bytes should be taken.
	 * @see #update(char[], int, int)
	 */
	private void update(char c[]) {
		update(c, 0, c.length);
	}

	/** Updates the digest with the given array fragment.
	 *
	 * <p>This method uses the {@linkplain #byteBuffer nternal byte buffer}
	 * to avoid calling {@link MessageDigest#update(byte[])} too many times.
	 *
	 * @param c the array from which bytes should be taken.
	 * @param offset the starting offset.
	 * @param length the number of character to be translated.
	 */
	private void update(char c[], int offset, int length) {
		for(int i = 0; i < length; i++) {
			if (fill == BYTE_BUFFER_SIZE) {
				md.update(byteBuffer);
				fill = 0;
			}
			byteBuffer[fill++] = (byte)(c[offset + i] >> 8);
			byteBuffer[fill++] = (byte)c[offset + i];
		}
	}


	/** Returns the digest computed.
	 *
	 * @return the digest computed.
	 *
	 */

	public byte[] digest() {
		return digest;
	}

	/** Sets the URI that will be used to tune the next digest.
	 *
	 * @param uri a URI, or {@code null} for no URL.
	 */

	public void url(URI uri) {
		this.url = uri;
	}

	public void startDocument() {
		md.reset();
		fill = 0;

		if (url != null) {
			update(url.getAuthority().toCharArray());
			update(AUTHORITY_DELIMITER);
		}
	}


	public boolean startElement(Element element, Map<Attribute, MutableString> attributes) {
		md.update(startTag.get(element)); // Bizarre elements

		if (element == Element.FRAME || element == Element.IFRAME) {
			final MutableString urlSpec = attributes.get(Attribute.SRC);
			if (urlSpec != null) {
				// TODO: Should we resolve?
				update(ATTRIBUTE_VALUE_DELIMITER);
				update(urlSpec.array(), 0, urlSpec.length());
				update(ATTRIBUTE_VALUE_DELIMITER);
			}
		}
		return true;
	}


	public boolean endElement(Element element) {
		md.update(endTag.get(element));
		return true;
	}


	public boolean characters(char[] data, int offset, int length, boolean flowBroken) {
		if (DEBUG) System.err.println(new String(data, offset, length));
		update(data, offset, length);
		return true;
	}


	public boolean cdata(Element element, char[] data, int offset, int length) {
		// TODO: maybe we want to use this
		return true;
	}


	public void endDocument() {
		md.update(byteBuffer, 0, fill);
		fill = 0;
		digest = md.digest();
	}
}

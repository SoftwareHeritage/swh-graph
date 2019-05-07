package it.unimi.dsi.law.warc.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.http.StatusLine;
import org.apache.http.message.BasicLineParser;
import org.apache.http.message.LineParser;
import org.apache.http.message.ParserCursor;
import org.apache.http.util.CharArrayBuffer;

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

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.law.bubing.util.BURL;
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;
import it.unimi.dsi.util.XorShift128PlusRandom;

// RELEASE-STATUS: DIST

/** Static utility methods. */

public class Util {
	private static final boolean ASSERTS = true;

	private Util() {}

	/** The strategy used to decide whether two header names are the same: we require that they are equal up to case. */
	public static final Hash.Strategy<String> CASE_INSENSITIVE_STRING_HASH_STRATEGY = new Hash.Strategy<String>() {
		public int hashCode(final String key) {
			int h = 0xDEAFC1CC;
			for(int i = key.length(); i-- != 0;) h ^= (h << 5) + Character.toLowerCase(key.charAt(i)) + (h >>> 2);
			return h;
		}

		public boolean equals(final String key0, final String key1) {
			return key0.equalsIgnoreCase(key1);
		}
	};

	/** Returns the given ASCII string as a byte array; characters are filtered through the 1111111(=0x7F) mask.
	 *
	 * @param s a string.
	 * @return <code>s</code> as a byte array.
	 */
	public static byte[] getASCIIBytes(String s) {
		final byte[] result = new byte[s.length()];
		if (ASSERTS) for (int i = result.length; i-- != 0;) assert s.charAt(i) < 0x80 : "Character at position  " + i + " is " + (int)s.charAt(i) + " in \"" + s + "\"";
		for (int i = result.length; i-- != 0;) result[i] = (byte)(s.charAt(i) & 0x7F);
		return result;
	}

	/** Returns the given ASCII mutable string as a byte array; characters are filtered through the 1111111(=0x7F) mask.
	 *
	 * @param s a mutable string.
	 * @return <code>s</code> as a byte array.
	 */

	public static byte[] getASCIIBytes(MutableString s) {
		final byte[] result = new byte[s.length()];
		final char[] a = s.array();
		if (ASSERTS) for (int i = result.length; i-- != 0;) assert a[i] < 0x80 : "Character at position  " + i + " is " + (int)a[i] + " in \"" + s + "\"";
		for (int i = result.length; i-- != 0;) result[i] = (byte)(a[i] & 0x7F);
		return result;
	}

	/** Returns the given byte array as an ASCII string. */
	public static String getString(byte[] array) {
		return getString(array, 0, array.length);
	}

	/** Returns the given byte array as an ASCII string. */
	public static String getString(byte[] array, int offset, int length) {
		if (ASSERTS) for (int j = length; j-- != 0;) assert array[offset + j] >= 0  : "Byte at position  " + (offset + j) + " is " + array[offset + j];
		int i = length;
		final char charArray[] = new char[i];
		while(i-- != 0) charArray[i] = (char)(array[offset + i] & 0x7F);
		return new String(charArray);
	}

	/** Returns &lfloor; log<sub>10</sub>(<code>x</code>) &rfloor;.
	 *
	 * @param x an integer.
	 * @return &lfloor; log<sub>10</sub>(<code>x</code>) &rfloor;, or -1 if <code>x</code> is smaller than or equal to zero.
	 */

	public static int log10(final int x) {
		return 	(x < 100000 ?
				(x < 100 ?
						(x < 10 ?
							(x < 1 ?
								-1 /* 4 */
							:
								0 /* 4 */
							)
						:
							1 /* 3 */
						)
					:
						(x < 10000 ?
							(x < 1000 ?
								2 /* 4 */
							:
								3 /* 4 */
							)
						:
							4 /* 3 */
						)
					)
				:
					(x < 100000000 ?
						(x < 10000000 ?
							(x < 1000000 ?
								5 /* 4 */
							:
								6 /* 4 */
							)
						:
							7 /* 3 */
						)
					:
						(x < 1000000000 ?
							8 /* 3 */
						:
							9 /* 3 */
						)
					)
				);
	}

	/** Returns &lfloor; log<sub>10</sub>(<code>x</code>) &rfloor;.
	 *
	 * @param x an integer.
	 * @return &lfloor; log<sub>10</sub>(<code>x</code>) &rfloor;, or -1 if <code>x</code> is smaller than or equal to zero.
	 */

	public static int log10(final long x) {
		return 	(x < 1000000000 ?
				(x < 10000 ?
						(x < 100 ?
							(x < 10 ?
								(x < 1 ?
									-1 /* 5 */
								:
									0 /* 5 */
								)
							:
								1 /* 4 */
							)
						:
							(x < 1000 ?
								2 /* 4 */
							:
								3 /* 4 */
							)
						)
					:
						(x < 10000000 ?
							(x < 1000000 ?
								(x < 100000 ?
									4 /* 5 */
								:
									5 /* 5 */
								)
							:
								6 /* 4 */
							)
						:
							(x < 100000000 ?
								7 /* 4 */
							:
								8 /* 4 */
							)
						)
					)
				:
					(x < 100000000000000L ?
						(x < 1000000000000L ?
							(x < 100000000000L ?
								(x < 10000000000L ?
									9 /* 5 */
								:
									10 /* 5 */
								)
							:
								11 /* 4 */
							)
						:
							(x < 10000000000000L ?
								12 /* 4 */
							:
								13 /* 4 */
							)
						)
					:
						(x < 100000000000000000L ?
							(x < 10000000000000000L ?
								(x < 1000000000000000L ?
									14 /* 5 */
								:
									15 /* 5 */
								)
							:
								16 /* 4 */
							)
						:
							(x < 1000000000000000000L ?
								17 /* 4 */
							:
								18 /* 4 */
							)
						)
					)
				);
	}

	/** Returns the number of decimal digits that are necessary to represent the argument.
	 *
	 * @param x a nonnegative integer.
	 * @return the number of decimal digits that are necessary to represent <code>x</code>.
	 */

	public static int digits(final int x) {
		if (ASSERTS) assert x >= 0 : x;
		if (x == 0) return 1;
		if (x > 1 << 30) return 10;
		return log10(x) + 1;
	}

	/** Returns the number of decimal digits that are necessary to represent the argument.
	 *
	 * @param x a nonnegative long.
	 * @return the number of decimal digits that are necessary to represent <code>x</code>.
	 */
	public static int digits(final long x) {
		if (ASSERTS) assert x >= 0 : x;
		if (x == 0) return 1;
		if (x > 1L << 62) return 19;
		return log10(x) + 1;
	}

	/** The given string is parsed as a comma-separated list of items, and the items are returned
	 *  in the form of an array, possibly after resolving an indirection. More precisely, <code>s</code>
	 *  is tokenized as a comma-separated list, and each item in the list is trimmed of all leading and trailing spaces. Then,
	 *  if the remaining character sequence does not start with <code>@</code>, it is interpreted literally;
	 *  otherwise, the <code>@</code> is stripped away and the remaining part is interpreted as a
	 *  URL or as a filename (depending on whether it is a valid URL or not), and the corresponding URL or file is in turn
	 *  read (ISO-8859-1 encoded) interpreted as a list of items, one per line, and the items are returned (literally)
	 *  after trimming all leading and trailing spaces. Lines that start with a # are ignored.
	 *
	 * @param s the property to be parsed.
	 * @return the array of items (as explaied above).
	 * @throws IOException if an exception is thrown while reading indirect items.
	 */
	public static String[] parseCommaSeparatedProperty(final String s) throws IOException {
		final StringTokenizer st = new StringTokenizer(s, ",");
		final List<String> result = new ArrayList<String>();
		String item;

		while (st.hasMoreTokens()) {
			item = st.nextToken().trim();
			if (item.length() > 1 && item.charAt(0) == '@') {
				String urlOrFilename = item.substring(1);
				URL url = new URL(urlOrFilename);
				BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream(), "ISO-8859-1"));
				while ((item = br.readLine()) != null) {
					if (item.startsWith("#")) continue;
					result.add(item.trim());
				}
				br.close();
			} else result.add(item);
		}
		return result.toArray(new String[0]);
	}

	/** Consumes a given number of bytes from a stream.
	 *
	 * @param in the stream.
	 * @param howMany the number of bytes to read, actually fewer bytes may be read if end of file is reached.
	 * @throws IOException
	 */
	public static void consume(InputStream in, long howMany) throws IOException {
		byte[] b = new byte[1024]; // just to read a bunch at a time
		long r;
		while (howMany > 0 && (r = in.read(b, 0, (int)Math.min(howMany, 1024))) != -1) howMany -= r;
	}

	/** Consumes all the bytes of a stream.
	 *
	 * @param in the stream.
	 * @throws IOException
	 */
	public static void consume(InputStream in) throws IOException {
		consume(in, Long.MAX_VALUE);
	}

	/**
     * Return byte array from an (unchunked) input stream.
     * Stop reading when <tt>"\n"</tt> terminator encountered
     * If the stream ends before the line terminator is found,
     * the last part of the string will still be returned.
     * If no input data available, {@code null} is returned.
     *
     * @param inputStream the stream to read from.
     * @param charset the charset used to decode the stream.
     *
     * @throws IOException if an I/O problem occurs
     * @return the read line.
     */
    public static String readHeaderLine(InputStream inputStream, Charset charset) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int ch;
        while ((ch = inputStream.read()) >= 0) {
            buf.write(ch);
            if (ch == '\n') break;  // be tolerant (RFC-2616 Section 19.3)
        }
        if (buf.size() == 0) return null;
        byte[] rawdata = buf.toByteArray();
        // strip CR and LF from the end
        int len = rawdata.length; // len > 0 since if buf was empty we already returned
        if (rawdata[len - 1] == '\n') {
            len--;
            if (len > 0 && rawdata[len - 1] == '\r') len--;
        }
        return new String(rawdata, 0, len, charset);
    }

    public static StatusLine readStatusLine(final MeasurableInputStream is, final Charset charset) throws IOException {
    	CharArrayBuffer buf = new CharArrayBuffer(64);
    	buf.append(readHeaderLine(is, charset));
    	LineParser parser = new BasicLineParser();
    	return parser.parseStatusLine(buf, new ParserCursor(0, buf.length()));
    }

    /**
     * Parses headers from the given stream.
     * Headers with the same name are not combined.
     *
     * @param is the stream to read headers from
     * @param map is the map where the headers will be saved
     * @param charset the charset to use for reading the data
     *
     * @throws IOException if an IO error occurs while reading from the stream
     */
	public static void readANVLHeaders(final MeasurableInputStream is, Map<String,String> map, final Charset charset) throws IOException, FormatException {
        String name = null;
        StringBuffer value = null;
        for (;;) {
            String line = readHeaderLine(is, charset);
            if (line == null || line.trim().length() < 1) break;
            // Parse the header name and value
            // Check for folded headers first
            // Detect LWS-char see HTTP/1.0 or HTTP/1.1 Section 2.2
            // discussion on folded headers
            if (line.charAt(0) == ' ' || line.charAt(0) == '\t') {
                // we have continuation folded header so append value
                if (value != null) {
                    value.append(' ');
                    value.append(line.trim());
                }
            } else {
                // make sure we save the previous name,value pair if present
                if (name != null) map.put(name, value.toString());
                // Otherwise we should have normal HTTP header line
                // Parse the header name and value
                int colon = line.indexOf(":");
                if (colon < 0) throw new FormatException("Unable to parse header: " + line);
                name = line.substring(0, colon).trim();
                value = new StringBuffer(line.substring(colon + 1).trim());
            }
        }
        // make sure we save the last name,value pair if present
        if (name != null) map.put(name, value.toString());
    }

	/**
	 * Writes a (name, value) map as an ANVL segment in a given stream.
	 *
	 * @param out the stream.
	 * @param map the map.
	 * @param charset the charset of the headers.
	 */
	public static void writeANVLHeaders(final OutputStream out, final Map<String, String> map, final Charset charset) {
		final CharsetEncoder encoder = charset.newEncoder();
		encoder.onMalformedInput(CodingErrorAction.IGNORE);
		encoder.onUnmappableCharacter(CodingErrorAction.IGNORE);
		final Writer writer = new OutputStreamWriter(out, encoder);
		try {
			for(Map.Entry<String, String> e: map.entrySet()) {
				writer.write(e.getKey());
				writer.write(": ");
				writer.write(e.getValue());
				writer.write("\r\n");
			}
			writer.close();
		}
		catch(IOException cantHappen) {
			throw new RuntimeException(cantHappen);
		}
	}

	/** Returns a mutable string representing in hexadecimal a digest.
	 *
	 * @param a a digest, as a byte array.
	 * @return a string hexadecimal representation of <code>a</code>.
	 */
	public static String toHexString(final byte[] a) {
		MutableString result = new MutableString(a.length * 2);
		for (int i = 0; i < a.length; i++)
			result.append((a[i] >= 0 && a[i] < 16 ? "0" : "")).append(Integer.toHexString(a[i] & 0xFF));
		return result.toString();
	}

	/** Returns a byte array corresponding to the given number.
	 *
	 * @param s the number, as a String.
	 * @return the byte array.
	 */
	public static byte[] fromHexString(final String s) {
		byte[] b = new byte[s.length() / 2];
		for (int i = s.length() / 2; i-- != 0;)
			b[i] = (byte)Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
		return b;
	}

	/** The random number generator used by {@link #createHierarchicalTempFile(File, int)}. */
	private static final XorShift128PlusRandom RND = new XorShift128PlusRandom();

	private static final Object CREATION_LOCK = new Object();

	/**
	 * Creates a temporary file with a random hierachical path.
	 *
	 * <p> A random hierarchical path of <var>n</var> path elements is a sequence of <var>n</var>
	 * directories of two hexadecimal digits each, followed by a filename created by {@link File#createTempFile(String, String, File)}.
	 *
	 * <p> This method creates an empty file having a random hierarchical path of the specified
	 * number of path elements under a given base directory, creating all needed directories along
	 * the hierarchical path (whereas the base directory is expected to already exist).
	 *
	 * @param baseDirectory the base directory (it must exist).
	 * @param pathElements the number of path elements (filename excluded), must be in [0,8]
	 * @param prefix will be passed to {@link File#createTempFile(String, String, File)}
	 * @param suffix will be passed to {@link File#createTempFile(String, String, File)}
	 * @return the temporary file.
	 * @throws IOException
	 */
	public static File createHierarchicalTempFile(final File baseDirectory, final int pathElements, final String prefix, final String suffix) throws IOException {
		if (! baseDirectory.isDirectory()) throw new IllegalArgumentException(baseDirectory + " is not a directory.");
		if (pathElements < 0 || pathElements > 8) throw new IllegalArgumentException();

		long x;
		synchronized (RND) { x = RND.nextLong(); }
		StringBuilder stringBuilder = new StringBuilder();
		for(int i = 0; i < pathElements; i++) {
			if (i != 0) stringBuilder.append(File.separatorChar);
			stringBuilder.append(Long.toHexString(x & 0xF));
			x >>= 4;
			stringBuilder.append(Long.toHexString(x & 0xF));
			x >>= 4;
		}

		File directory = baseDirectory;
		if (pathElements > 0) {
			directory = new File(baseDirectory, stringBuilder.toString());
			synchronized (CREATION_LOCK) {
				if ((directory.exists() && ! directory.isDirectory()) || (! directory.exists() && ! directory.mkdirs())) throw new IOException("Cannot create directory " + directory);
			}
		}

		return File.createTempFile(prefix, suffix, directory);
	}

	private static final char[] RESERVED = new char[] { '[', ']', '"', '|', '{', '}', '^', '<', '>', '`' };

	private static final String[] RESERVED_SUBST;
	static {
		RESERVED_SUBST = new String[RESERVED.length];
		for(int i = RESERVED_SUBST.length; i-- != 0;) RESERVED_SUBST[i] = (RESERVED[i] < 16 ? "%0" : "%") + Integer.toHexString(RESERVED[i]);
	}

	/** Fixes a given URL so that it is {@link BURL}-parsable.
	 *
	 * @param url a URL, possibly with bad characters in its path.
	 */


	public static void fixURL(final MutableString url) {
		// If they used %27 for the slash, fix it.
		if (url.startsWith("http:%2F%2F")) url.replace("%2F", "/");
		if (url.startsWith("http:%2f%2f")) url.replace("%2f", "/");
		// If they wrote http:/<host>, fix it.
		final int prefix = "http:/".length();
		if (url.startsWith("http:/") && url.length() > prefix && url.charAt(prefix) != '/') url.insert(prefix, '/');
		// If the last characters is a quote, eliminate it.
		if (url.lastChar() == '"' || url.lastChar() == '\'') url.length(url.length() -1);
		// Replace reserved characters (blindly)
		url.replace(RESERVED, RESERVED_SUBST);

		// Find percents not followed by two hexadecimal digits.
		final char[] a = url.array();
		final int l = url.length();
		for(int i = l; i-- != 0;) {
			if (a[i] == '%' && (i >= l - 2 || ! isHexDigit(a[i + 1]) || ! isHexDigit(a[i + 2]))) url.insert(i + 1, "25");
		}
	}

	private static boolean isHexDigit(char c) {
		return c >= '0' && c <= '9' || c >= 'A' && c <= 'F' || c >= 'a' && c <= 'f';
	}

	/** Checks if the given File exists and is a directory, or if not existent, it makes a directory (and its parent). */
	public static boolean ensureDirectory(File dir) {
		if (dir.exists() && ! dir.isDirectory()) return false;
		if (! dir.exists()) dir.mkdirs();
		return true;
	}

}

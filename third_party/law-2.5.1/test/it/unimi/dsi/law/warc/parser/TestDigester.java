package it.unimi.dsi.law.warc.parser;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.law.bubing.util.BURL;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.util.AbstractHttpResponse;



//RELEASE-STATUS: DIST

@SuppressWarnings("deprecation")
public class TestDigester {

	public static class FakeHttpResponse extends AbstractHttpResponse {
		MeasurableInputStream in;
		URI uri;
		final StatusLine STATUS_LINE = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 0), 200, "OK");
		protected FakeHttpResponse(URI uri, MeasurableInputStream in) {
			this.uri = uri;
			this.in = in;
		}
		public int status() { return 200; }
		public StatusLine statusLine() { return STATUS_LINE; }
		public Map<String, String> headers() { return new HashMap<String,String>(); }
		public MeasurableInputStream contentAsStream() throws IOException { return in; }
		public URI uri() { return uri; }
		public boolean fromWarcRecord(WarcRecord wr) throws IOException { throw new UnsupportedOperationException(); }

	}

	public final static String document1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document2Like1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/kxxx.php\";\n" + // Change, not relevant
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<tiTLE id=\"mummu\" special-type=\"liturchi\">Sebastiano Vigna</title>\n" + // Change, not relevant
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring xxxxediqne\"> and not this one\n" + // Change, not relevant
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document3Unlike1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye THIS IS A DIFFERENCE IN THE TEXT bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document4Unlike1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n " + //A SMALL DIFFERENCE: just a whitespace
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document5Unlike1 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"a/aFrameSource\">The frame source counts</frame>\n" + // A difference in the source should count!
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document6Like5 = // Should be the same as document5Unlike1, if URL of the latter is xxx/a and of this is xxx
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n" +
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"aFrameSource\">The frame source counts</frame>\n" + // A difference in the source should count!
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";

	public final static String document7prefix =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<title id=\"mamma\" special-type=\"li turchi\">Sebastiano Vigna</title>\n" +
		"</HEAD>\n" +
		"<boDY>\n" +
		"<div id=header>:::Sebastiano Vigna</div>" +
		"<div id=left>\n";

	public final static String document7suffix =
		"<ul id=\"left-nav\">" +
		"<br>Bye bye baby\n" +
		"<img SRc=\"but I'm ignoring this one\"> and not this one\n" +
		"\n\n even whitespace counts \n\n" +
		"<frame SRC=\"http://www.GOOGLE.com/\">The frame source counts</frame>\n" +
		"<iframe SRC=\"http://www.GOOGLE.com/\">And so does the iframe source</iframe>\n" +
		"</body>\n" +
		"</html>";


	private static String[] allDocs = { document1, document2Like1, document3Unlike1, document4Unlike1, document5Unlike1, document6Like5 };
	private static String[] allURLs = { "http://vigna.dsi.unimi.it/xxx/yyy/a.html", "http://vigna.dsi.unimi.it/", "http://vigna.dsi.unimi.it/bbb", "http://vigna.dsi.unimi.it/bbb.php", "http://vigna.dsi.unimi.it/a", "http://vigna.dsi.unimi.it/" };

	@Test
	public void testDocument1() throws NoSuchAlgorithmException, IOException {
		HTMLParser parser = new HTMLParser(MessageDigest.getInstance("MD5"));

		byte[][] allDigests = new byte[allDocs.length][];

		for (int i = 0; i < allDocs.length; i++) {
			allDigests[i] = parser.parse(new FakeHttpResponse(BURL.parse(allURLs[i]), new FastBufferedInputStream(new FastByteArrayInputStream(allDocs[i].getBytes()))), Parser.NULL_LINK_RECEIVER);
		}
		assertTrue(Arrays.equals(allDigests[0], allDigests[1]));
		assertFalse(Arrays.equals(allDigests[0], allDigests[2]));
		assertFalse(Arrays.equals(allDigests[0], allDigests[3]));
		assertFalse(Arrays.equals(allDigests[0], allDigests[4]));
		/* FIXME currently the next test fails because the derelativization feature of the SRC by Digester is not implemented; please
		 * uncomment the following line as soon as it is re-implemented
		 */
		//assertTrue(Arrays.equals(allDigests[4], allDigests[5]));
	}

	public void assertSameDigest(String a, String b) throws NoSuchAlgorithmException, IOException {
		assertDigest(BURL.parse("http://a"), a, BURL.parse("http://a"), b, true);
	}

	public void assertDifferentDigest(String a, String b) throws NoSuchAlgorithmException, IOException {
		assertDigest(BURL.parse("http://a"), a, BURL.parse("http://a"), b, false);
	}

	public void assertDigest(URI prefixa, String a, URI prefixb, String b, boolean equal) throws NoSuchAlgorithmException, IOException {
		HTMLParser parser = new HTMLParser(MessageDigest.getInstance("MD5"));
		final byte[] digest0 = parser.parse(new FakeHttpResponse(prefixa, new FastBufferedInputStream(new FastByteArrayInputStream(a.getBytes()))), Parser.NULL_LINK_RECEIVER);
		final byte[] digest1 = parser.parse(new FakeHttpResponse(prefixb, new FastBufferedInputStream(new FastByteArrayInputStream(b.getBytes()))), Parser.NULL_LINK_RECEIVER);
		assertEquals(Boolean.valueOf(Arrays.equals(digest0, digest1)), Boolean.valueOf(equal));
	}

	@Test
	public void testDifferent() throws NoSuchAlgorithmException, IOException {
		assertDifferentDigest("a", "b");
		assertDifferentDigest("<a>", "<i>");
		assertDifferentDigest("<foo>", "</foo>");
		assertDifferentDigest("<frame src=a>", "<frame src=b>");
		assertDifferentDigest("<iframe src=a>", "<iframe src=b>");
		assertDigest(BURL.parse("http://a"), "x", BURL.parse("http://b"), "x", false);
	}

	@Test
	public void testSame() throws NoSuchAlgorithmException, IOException {
		assertSameDigest("<a b>", "<a c>");
		assertSameDigest("<foo>", "<bar>");
		assertSameDigest("<foo >", "<foo  >");
		assertSameDigest("<img src=a>", "<img src=b>");
		assertSameDigest("<i>ciao mamma</i>", "<I>ciao mamma</I>");
		assertDigest(BURL.parse("http://a"), "x", BURL.parse("http://a"), "x", true);
	}

	@Test
	public void testLongDocument() throws NoSuchAlgorithmException, IOException  {
		Random r = new Random(0);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < HTMLParser.CHAR_BUFFER_SIZE * (2 + r.nextInt(3)); i++) sb.append((char)(64 + r.nextInt(61)));
		final String document7 = document7prefix + sb.toString() + document7suffix;
		assertSameDigest(document7, document7);
		sb.setCharAt(sb.length() / 2, (char)(sb.charAt(sb.length() / 2) + 1));
		assertDifferentDigest(document7, sb.toString());
	}


}

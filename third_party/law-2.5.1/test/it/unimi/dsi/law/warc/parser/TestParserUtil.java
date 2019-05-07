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

import org.junit.Test;



//RELEASE-STATUS: DIST

public class TestParserUtil {

	private static final String documentNoMeta =
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

	private static final String documentMetaNeverClosed =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<META" +
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

	private static final String documentMetaNeverClosed2 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<META                     http-equiv         =cacca" +
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

	private static final String documentMetaIsutf_8 =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8\" >" +
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

	private static final String documentMetaIsutf_8ButNotClosed =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8" +
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

	private static final String documentMetaIsutf_8dAndSomething =
		"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Strict//EN\" \"http://www.w3.org/TR/REC-html40/strict.dtd\">\n" +
		"\n" +
		"<html>\n" +
		"<head>\n" +
		"<style type=\"text/css\">\n" +
		"@import \"/css/content.php\";\n" +
		"@import \"/css/layout.php\";\n" +
		"</style>" +
		"<meta http-equiv=\"Content-Type\" content=\"text/html;charset=utf-8d etc\">" +
		" and something\n maybe on a new line...<META                     http-equiv         =\"content-type\" " +
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

	@Test
	public void testGetCharsetName() {
		byte[] b;
		b = documentNoMeta.getBytes();
		assertEquals(null, HTMLParser.getCharsetName(b, b.length));
		b = documentMetaNeverClosed.getBytes();
		assertEquals(null, HTMLParser.getCharsetName(b, b.length));
		b = documentMetaNeverClosed2.getBytes();
		assertEquals(null, HTMLParser.getCharsetName(b, b.length));
		b = documentMetaIsutf_8.getBytes();
		assertEquals("utf-8", HTMLParser.getCharsetName(b, b.length));
		b = documentMetaIsutf_8ButNotClosed.getBytes();
		assertEquals("utf-8", HTMLParser.getCharsetName(b, b.length));
		b = documentMetaIsutf_8dAndSomething.getBytes();
		assertEquals("utf-8d", HTMLParser.getCharsetName(b, b.length));
	}
}

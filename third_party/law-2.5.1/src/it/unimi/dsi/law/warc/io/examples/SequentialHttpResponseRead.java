package it.unimi.dsi.law.warc.io.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

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

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.law.warc.io.GZWarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.util.HttpResponse;
import it.unimi.dsi.law.warc.util.WarcHttpResponse;

// RELEASE-STATUS: DIST

public class SequentialHttpResponseRead {

	final static int IO_BUFFER_SIZE = 64 * 1024;

	public static void main(String arg[]) throws Exception {

		final String warcFile = "test";
		final boolean isGZipped = true;

		final WarcRecord record = isGZipped ? new GZWarcRecord() : new WarcRecord();
		final WarcHttpResponse response = new WarcHttpResponse();

		final FastBufferedInputStream in = new FastBufferedInputStream(
				new FileInputStream(new File(warcFile + ".warc" + (isGZipped ? ".gz" : ""))), IO_BUFFER_SIZE);

		for (;;) {

			if (record.read(in) == -1) break;
			if (isGZipped) System.out.println("GZip header:\n" + ((GZWarcRecord)record).gzheader);
			System.out.println("WARC header:\n" + record.header);

			if (! response.fromWarcRecord(record)) continue;

			System.out.println("HTTP status line:\n" + response.statusLine());
			System.out.println("HTTP headers:\n" + response.headers());

			System.out.println("First few bytes of content:");

			Charset charset = Charsets.ISO_8859_1;
			String charsetName = record.header.anvlFields.get(HttpResponse.GUESSED_CHARSET_HEADER);
			if (charsetName != null) try {
				charset = Charset.forName(charsetName);
			} catch (IllegalCharsetNameException e) {
				System.err.println("Illegal charset, using " + Charsets.ISO_8859_1);
			} catch (UnsupportedCharsetException e) {
				System.err.println("Unsupported charset, using " + Charsets.ISO_8859_1);
			}

			final Reader reader = new InputStreamReader(response.contentAsStream(), charset);
			int n = 100, r;
			while ((r = reader.read()) != -1 && n-- > 0)
				System.out.print((char)r);

			System.out.println("\n");

		}

	}
}

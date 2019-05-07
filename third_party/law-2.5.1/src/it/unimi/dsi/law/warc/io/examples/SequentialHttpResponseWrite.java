package it.unimi.dsi.law.warc.io.examples;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;

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

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.law.bubing.util.BURL;
import it.unimi.dsi.law.warc.io.GZWarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.util.MutableHttpResponse;
import it.unimi.dsi.law.warc.util.Util;

// RELEASE-STATUS: DIST

public class SequentialHttpResponseWrite {

	final static int IO_BUFFER_SIZE = 64 * 1024;

	public static void main(String arg[]) throws Exception {

		final String warcFile = "test";
		final boolean isGZipped = true;

		final WarcRecord record = isGZipped ? new GZWarcRecord() : new WarcRecord();
		final MutableHttpResponse response = new MutableHttpResponse();

		final FastBufferedOutputStream out = new FastBufferedOutputStream(
				new FileOutputStream(new File(warcFile + ".warc" + (isGZipped ? ".gz" : ""))), IO_BUFFER_SIZE);

		for (int i = 0; i < 10; i++) {

			response.statusLine(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 0), 200, "OK"));
			response.uri(BURL.parse("http://localhost/" + i));
			response.headers(null);
			response.contentAsStream(new FastByteArrayInputStream(Util.getASCIIBytes("<html><head><title>Doc " + i + "</title><body><p>This is document nr. " + i + "</body></html>")));

			response.toWarcRecord(record);
			record.write(out);

		}

		out.close();

	}
}

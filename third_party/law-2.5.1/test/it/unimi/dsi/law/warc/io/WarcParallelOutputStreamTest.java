package it.unimi.dsi.law.warc.io;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.NullOutputStream;
import it.unimi.dsi.law.bubing.util.MockResponses.MockHttpResponseFromString;
import it.unimi.dsi.law.warc.io.TestGZWarcRecord.MockGZWarcRecord;
import it.unimi.dsi.law.warc.io.TestWarcRecord.MockWarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;

//RELEASE-STATUS: DIST

public class WarcParallelOutputStreamTest {
	public static final boolean DEBUG = false;

	@Test
	public void testRecord() throws IOException, FormatException, InterruptedException {


		for(boolean gzip: new boolean[] { true, false }) {
			final FastByteArrayOutputStream out = new FastByteArrayOutputStream();
			final WarcParallelOutputStream warcParallelOutputStream = new WarcParallelOutputStream(out, gzip);
			final Thread thread[] = new Thread[4];

			for(int i = 0; i < thread.length; i++)
				(thread[i] = new Thread(Integer.toString(i)) {
					public void run() {
						final int index = Integer.parseInt(getName());
						for (int i = index * (1000 / thread.length); i < (index + 1) * (1000 / thread.length); i++) {
							try {
								final WarcRecord warcRecord = warcParallelOutputStream.acquire();
								MockHttpResponseFromString response = new MockHttpResponseFromString(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"), new Header[0], URI.create("http://example.com/" + (1000 + i)), "X");
								response.toWarcRecord(warcRecord);
								warcParallelOutputStream.release(warcRecord);
							} catch(Exception e) {}
						}
					}
				}).start();


			for(Thread t: thread) t.join();
			warcParallelOutputStream.close();
			out.close();

			final WarcRecord warcRecord = new WarcRecord();
			MockHttpResponseFromString response = new MockHttpResponseFromString(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"), new Header[0], URI.create("http://example.com/" + 1000), "X");
			response.toWarcRecord(warcRecord);
			warcRecord.write(new FastBufferedOutputStream(NullOutputStream.getInstance()));

			FastBufferedInputStream in = new FastBufferedInputStream(new FastByteArrayInputStream(out.array, 0, out.length));
			final boolean found[] = new boolean[1000];
			if (gzip) {
				final MockGZWarcRecord readRecord = new MockGZWarcRecord();
				for (int i = 0; i < 1000; i++) {
					readRecord.read(in);
					assertEquals(readRecord.header.dataLength, warcRecord.header.dataLength);
					found[Integer.parseInt(readRecord.header.subjectUri.getPath().substring(1)) - 1000] = true;
					IOUtils.contentEquals(warcRecord.block, readRecord.actualBlock());
					readRecord.checkCRC(in);
				}
			}
			else {
				final MockWarcRecord readRecord = new MockWarcRecord();
				for (int i = 0; i < 1000; i++) {
					readRecord.read(in);
					assertEquals(readRecord.header.dataLength, warcRecord.header.dataLength);
					found[Integer.parseInt(readRecord.header.subjectUri.getPath().substring(1)) - 1000] = true;
					IOUtils.contentEquals(warcRecord.block, readRecord.actualBlock());
				}
			}
			in.close();

			for(int i = 1000; i-- != 0;) assertTrue(Integer.toString(i), found[i]);
		}
	}
}

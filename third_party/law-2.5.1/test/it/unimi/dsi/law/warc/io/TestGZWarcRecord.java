package it.unimi.dsi.law.warc.io;

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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.io.MeasurableInputStream;
import it.unimi.dsi.law.bubing.util.MockResponses.MockRandomHttpResponse;
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;
import it.unimi.dsi.law.warc.util.AbstractHttpResponse;
import it.unimi.dsi.law.warc.util.WarcHttpResponse;
import it.unimi.dsi.logging.ProgressLogger;


//RELEASE-STATUS: DIST


/** A class to test {@link GZWarcRecord}. */

public class TestGZWarcRecord {
	private final static Logger LOGGER = LoggerFactory.getLogger(TestGZWarcRecord.class);

	public static final boolean DEBUG = false;
	private final static Random RND = new Random(0);

	static public class MockGZWarcRecord extends GZWarcRecord {
		private final static int DEFAULT_MAX_BLOCK_LENGTH = 1024;
		private static int calls = 0;
		private final byte[] blockBytes;
		public MockGZWarcRecord() {
			TestWarcRecord.rndFill(header, calls++);
			blockBytes = TestWarcRecord.rndBlock(DEFAULT_MAX_BLOCK_LENGTH);
			block = new FastByteArrayInputStream(blockBytes);
		}
		public MeasurableInputStream expectedBlock() {
			return new FastByteArrayInputStream(blockBytes);
		}
		public MeasurableInputStream actualBlock() {
			return block; // this will be overwritten by read
		}
	}

	final static int NUM_WRS_TESTS = 100;

	@Test
	public void testRecord() throws IOException, FormatException {

		System.err.print("Test on mock random gzip records: ");

		/* a place to remember written HttpResponses and WarcRecods. */

		final ArrayList<MockGZWarcRecord> writtenRecords = new ArrayList<MockGZWarcRecord>();

		/* write and remember */

		final FastByteArrayOutputStream out = new FastByteArrayOutputStream();
		for (int i = 0; i < NUM_WRS_TESTS; i++) {
			final MockGZWarcRecord writtenRecord = new MockGZWarcRecord();
			writtenRecord.write(out);
			writtenRecords.add(writtenRecord);
			System.err.print("w");
		}
		out.close();

		System.err.print("/");

		/* a place to read */

		final MockGZWarcRecord readRecord = new MockGZWarcRecord();

		/* read and compare */

		FastBufferedInputStream in = new FastBufferedInputStream(new FastByteArrayInputStream(out.array, 0, out.length));
		for (int i = 0; i < NUM_WRS_TESTS; i++) {

			final MockGZWarcRecord writtenRecord = writtenRecords.get(i);

			if (RND.nextBoolean()) { // read

				readRecord.read(in);

				if (DEBUG) System.err.println("\n" + readRecord.header + "\n" + writtenRecord.header + "\n" + readRecord.gzheader + "\n" + writtenRecord.gzheader);

				assertEquals(readRecord.header, writtenRecord.header);

				if (RND.nextBoolean()) { // partial read

					final MeasurableInputStream block = readRecord.actualBlock();
					readRecord.actualBlock().read(new byte[RND.nextInt((int)(block.length() / 2)) + 1]);

					System.err.print("r");

				} else { // consume block

					IOUtils.contentEquals(writtenRecord.expectedBlock(), readRecord.actualBlock());
					System.err.print("R");

				}

				if (RND.nextBoolean()) { // checkCRC

					readRecord.checkCRC(in);
					System.err.print("c");

				} else System.err.print(".");

			} else { // skip

				long length = readRecord.skip(in);
				assertEquals(writtenRecord.gzheader.compressedSkipLength, length);
				System.err.print("s");

			}

		}
		in.close();

		System.err.println(" done.");
	}

	@Test
	public void testResponse() throws IOException, FormatException {

		System.err.print("Test on mock random gzip repsonces: ");

		/* a place to remember written HttpResponses and WarcRecods. */

		final ArrayList<MockRandomHttpResponse> writtenResponses = new ArrayList<MockRandomHttpResponse>();
		final ArrayList<GZWarcRecord> writtenGZRecords = new ArrayList<GZWarcRecord>();

		/* write and remember */

		final FastByteArrayOutputStream out = new FastByteArrayOutputStream();

		for (int i = 0; i < NUM_WRS_TESTS; i++) {
			final MockRandomHttpResponse writtenResponse = new MockRandomHttpResponse(RND);
			final GZWarcRecord writtenRecord = new GZWarcRecord();
			writtenResponses.add(writtenResponse);
			writtenResponse.toWarcRecord(writtenRecord);
			if (RND.nextBoolean()) {
				writtenRecord.header.anvlFields.clear();
				writtenRecord.header.anvlFields.put("anvl-test-key", "anvl-test-value");
			}

			writtenRecord.write(out);
			writtenGZRecords.add(writtenRecord);
			System.err.print("w");
		}
		out.close();

		System.err.print("/");

		/* a place to read */

		WarcHttpResponse readResponse = new WarcHttpResponse();
		GZWarcRecord readRecord = new GZWarcRecord();

		/* read and compare */

		FastBufferedInputStream in = new FastBufferedInputStream(new FastByteArrayInputStream(out.array, 0, out.length));
		for (int i = 0; i < NUM_WRS_TESTS; i++) {

			final GZWarcRecord writtenGZRercord = writtenGZRecords.get(i);

			if (RND.nextBoolean()) { // read

				readRecord.read(in);
				assertEquals(writtenGZRercord.header, readRecord.header);
				assertEquals(writtenGZRercord.gzheader, readRecord.gzheader);

				if (RND.nextBoolean()) { // don't consume block

					System.err.print("r");

				} else { // consume block

					final MockRandomHttpResponse writtenResponse = writtenResponses.get(i);
					readResponse.fromWarcRecord(readRecord);
					assertTrue(IOUtils.contentEquals(writtenResponse.expectedContentAsStream(), readResponse.contentAsStream()));

					System.err.print("R");

				}

			} else { // skip

				long length = readRecord.skip(in);
				assertEquals(writtenGZRercord.gzheader.compressedSkipLength, length);
				System.err.print("s");

			}

		}
		in.close();

		System.err.println(" done.");
	}

	final static int IO_BUFFER_SIZE = 64 * 1024;
	public static void main(String[] arg) throws FileNotFoundException, IOException, FormatException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP(TestWarcRecord.class.getName(), "GZWarcRecord performance test.",
				new Parameter[] {
					new UnflaggedOption("numPages", JSAP.INTEGER_PARSER, "10000", JSAP.REQUIRED, false, "The number of pages to write."),
					new UnflaggedOption("maxPageSize", JSAP.INTEGER_PARSER, "1024", JSAP.REQUIRED, false, "The maximum size of page content."),
			});
		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		int numPages = jsapResult.getInt("numPages");
		int maxPageSize = jsapResult.getInt("maxPageSize");

		File tmp = File.createTempFile("warctest-", ".gz");
		FastBufferedOutputStream out = new FastBufferedOutputStream(new FileOutputStream(tmp), IO_BUFFER_SIZE);

		GZWarcRecord gzwr = new GZWarcRecord();

		ProgressLogger pl = new ProgressLogger(LOGGER, "responses");
		pl.start("Generating/Writing gzip mock responses (in '" + tmp + "')...");
		for (int i = 0; i < numPages; i++) {
			AbstractHttpResponse r = new MockRandomHttpResponse(RND, maxPageSize);
			r.toWarcRecord(gzwr);
			gzwr.write(out);
			pl.update();
		}
		pl.done();
		out.close();

		FastBufferedInputStream in = new FastBufferedInputStream(new FileInputStream(tmp), IO_BUFFER_SIZE);
		WarcHttpResponse whr = new WarcHttpResponse();
		pl.start("Reading gzip responses...");
		gzwr.resetRead();
		for (int i = 0; i < numPages; i++) {
			gzwr.read(in);
			whr.fromWarcRecord(gzwr);
			pl.update();
		}
		pl.done();
		in.close();

		pl.itemsName = "records";
		in = new FastBufferedInputStream(new FileInputStream(tmp), IO_BUFFER_SIZE);
		pl.start("Skipping gzip records...");
		gzwr.resetRead();
		for (int i = 0; i < numPages; i++) {
			gzwr.read(in);
			whr.fromWarcRecord(gzwr);
			pl.update();
		}
		pl.done();
		in.close();

	}



}


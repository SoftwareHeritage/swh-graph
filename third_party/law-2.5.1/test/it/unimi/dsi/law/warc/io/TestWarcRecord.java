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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

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
import it.unimi.dsi.law.bubing.util.BURL;
import it.unimi.dsi.law.bubing.util.MockResponses.MockRandomHttpResponse;
import it.unimi.dsi.law.warc.io.WarcRecord.ContentType;
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;
import it.unimi.dsi.law.warc.io.WarcRecord.RecordType;
import it.unimi.dsi.law.warc.util.AbstractHttpResponse;
import it.unimi.dsi.law.warc.util.WarcHttpResponse;
import it.unimi.dsi.logging.ProgressLogger;


//RELEASE-STATUS: DIST


/** A class to test {@link WarcRecord}. */

@SuppressWarnings("deprecation")
public class TestWarcRecord {
	private final static Logger LOGGER = LoggerFactory.getLogger(TestWarcRecord.class);

	public static final boolean DEBUG = false;
	private final static Random RND = new Random(0);

	public final static <E extends Enum<E>> E rndEnum(E[] values) {
		return values[RND.nextInt(values.length)];
	}

	public final static void rndFill(WarcRecord.Header header, int calls) {
		header.dataLength = -1;
		header.recordType = rndEnum(RecordType.values());
		header.subjectUri = BURL.parse("http" + (RND.nextBoolean() ? "s" : "") + "://this.is/n" + calls + "/test.html");
		header.creationDate = new Date();
		header.contentType = rndEnum(ContentType.values());
		header.recordId = UUID.randomUUID();
		if (RND.nextBoolean()) {
			header.anvlFields.clear();
			header.anvlFields.put("anvl-test-key", "anvl-test-value");
		}
	}

	public final static byte[] rndBlock(int maxLen) {
		byte[] blockBytes = new byte[RND.nextInt(maxLen) + 1];
		RND.nextBytes(blockBytes);
		return blockBytes;
	}

	static public class MockWarcRecord extends WarcRecord {
		private final static int DEFAULT_MAX_BLOCK_LENGTH = 1024;
		private static int calls = 0;
		private final byte[] blockBytes;
		public MockWarcRecord() {
			rndFill(header, calls++);
			blockBytes = rndBlock(DEFAULT_MAX_BLOCK_LENGTH);
			block = new FastByteArrayInputStream(blockBytes);
		}
		public MeasurableInputStream expectedBlock() {
			return new FastByteArrayInputStream(blockBytes);
		}
		public MeasurableInputStream actualBlock() {
			return block; // this will be overwritten by read
		}
	}

	final static int NUM_SIZE_TESTS = 100;

	@Test
	public void testSize() throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		WarcRecord wr = new WarcRecord();

		System.err.print("Size test on mock random repsonces: ");
		for (int rep = 0; rep < NUM_SIZE_TESTS; rep++) {
			out.reset();
			AbstractHttpResponse r = new MockRandomHttpResponse(RND);
			r.toWarcRecord(wr);
			if (RND.nextBoolean()) {
				wr.header.anvlFields.clear();
				wr.header.anvlFields.put("anvl-test-key", "anvl-test-value");
			}
			wr.write(out);
			byte[] written = out.toByteArray();
			int i = WarcRecord.WARC_ID.length + 1, size = 0;
			while (written[i] != ' ') {
				size = 10 * size + (written[i] - '0');
				i++;
			}
			if (DEBUG) System.out.println(out.toString() + "\nActual size: " + out.size() + "\nParsed size: " + size);
			assertEquals(out.size(), size);
			System.err.print(".");
		}
		System.err.println(" done.");
	}

	final static int NUM_WRS_TESTS = 100;

	@Test
	public void testRecord() throws IOException, FormatException {

		System.err.print("Test on mock random records: ");

		/* a place to remember written HttpResponses and WarcRecods. */

		final ArrayList<MockWarcRecord> writtenRecords = new ArrayList<MockWarcRecord>();

		/* write and remember */

		final FastByteArrayOutputStream out = new FastByteArrayOutputStream();
		for (int i = 0; i < NUM_WRS_TESTS; i++) {
			final MockWarcRecord writtenRecord = new MockWarcRecord();
			writtenRecord.write(out);
			writtenRecords.add(writtenRecord);
			System.err.print("w");
		}
		out.close();

		System.err.print("/");

		/* a place to read */

		final MockWarcRecord readRecord = new MockWarcRecord();

		/* read and compare */

		FastBufferedInputStream in = new FastBufferedInputStream(new FastByteArrayInputStream(out.array, 0, out.length));
		for (int i = 0; i < NUM_WRS_TESTS; i++) {

			final MockWarcRecord writtenRecord = writtenRecords.get(i);

			if (RND.nextBoolean()) { // read

				readRecord.read(in);

				if (DEBUG) System.err.println("\n" + readRecord.header + "\n" + writtenRecord.header);

				assertEquals(readRecord.header, writtenRecord.header);

				if (RND.nextBoolean()) { // don't consume block

					final MeasurableInputStream block = readRecord.actualBlock();
					readRecord.actualBlock().read(new byte[RND.nextInt((int)(block.length() / 2)) + 1]);

					System.err.print("r");

				} else { // consume block

					IOUtils.contentEquals(writtenRecord.expectedBlock(), readRecord.actualBlock());
					System.err.print("R");

				}

			} else { // skip

				long length = readRecord.skip(in);
				assertEquals(writtenRecord.header.dataLength, length);

				System.err.print("s");

			}

		}
		in.close();

		System.err.println(" done.");
	}

	@Test
	public void testResponse() throws IOException, FormatException {

		System.err.print("Test on mock random repsonces: ");

		/* a place to remember written HttpResponses and WarcRecods. */

		final ArrayList<MockRandomHttpResponse> writtenResponses = new ArrayList<MockRandomHttpResponse>();
		final ArrayList<WarcRecord> writtenRecords = new ArrayList<WarcRecord>();

		/* write and remember */

		final FastByteArrayOutputStream out = new FastByteArrayOutputStream();

		for (int i = 0; i < NUM_WRS_TESTS; i++) {
			final MockRandomHttpResponse writtenResponse = new MockRandomHttpResponse(RND);
			final WarcRecord writtenRecord = new WarcRecord();
			writtenResponses.add(writtenResponse);
			writtenResponse.toWarcRecord(writtenRecord);
			if (RND.nextBoolean()) {
				writtenRecord.header.anvlFields.clear();
				writtenRecord.header.anvlFields.put("anvl-test-key", "anvl-test-value");
			}
			writtenRecord.write(out);
			writtenRecords.add(writtenRecord);
			System.err.print("w");
		}
		out.close();

		System.err.print("/");

		/* a place to read */

		WarcHttpResponse readResponse = new WarcHttpResponse();
		WarcRecord readRecord = new WarcRecord();

		/* read and compare */

		FastBufferedInputStream in = new FastBufferedInputStream(new FastByteArrayInputStream(out.array, 0, out.length));
		for (int i = 0; i < NUM_WRS_TESTS; i++) {

			final WarcRecord writtenRercord = writtenRecords.get(i);

			if (RND.nextBoolean()) { // read

				readRecord.read(in);
				assertEquals(writtenRercord.header, readRecord.header);

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
				assertEquals(writtenRercord.header.dataLength, length);
				System.err.print("s");

			}

		}
		in.close();

		System.err.println(" done.");
	}

	final static int IO_BUFFER_SIZE = 64 * 1024;
	public static void main(String[] arg) throws FileNotFoundException, IOException, FormatException, JSAPException {
		SimpleJSAP jsap = new SimpleJSAP(TestWarcRecord.class.getName(), "WarcRecord performance test.",
				new Parameter[] {
					new UnflaggedOption("numPages", JSAP.INTEGER_PARSER, "10000", JSAP.REQUIRED, false, "The number of pages to write."),
					new UnflaggedOption("maxPageSize", JSAP.INTEGER_PARSER, "1024", JSAP.REQUIRED, false, "The maximum size of page content."),
			});
		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		int numPages = jsapResult.getInt("numPages");
		int maxPageSize = jsapResult.getInt("maxPageSize");

		File tmp = File.createTempFile("warctest-", null);
		FastBufferedOutputStream out = new FastBufferedOutputStream(new FileOutputStream(tmp), IO_BUFFER_SIZE);

		WarcRecord wr = new WarcRecord();

		ProgressLogger pl = new ProgressLogger(LOGGER, "responses");
		pl.start("Generating/Writing mock responses...");
		for (int i = 0; i < numPages; i++) {
			AbstractHttpResponse r = new MockRandomHttpResponse(RND, maxPageSize);
			r.toWarcRecord(wr);
			wr.write(out);
			pl.update();
		}
		pl.done();
		out.close();

		FastBufferedInputStream in = new FastBufferedInputStream(new FileInputStream(tmp), IO_BUFFER_SIZE);
		WarcHttpResponse whr = new WarcHttpResponse();
		pl.start("Reading responses...");
		wr.resetRead();
		for (int i = 0; i < numPages; i++) {
			wr.read(in);
			whr.fromWarcRecord(wr);
			IOUtils.toByteArray(whr.contentAsStream());
			pl.update();
		}
		pl.done();
		in.close();

		pl.itemsName = "records";
		in = new FastBufferedInputStream(new FileInputStream(tmp), IO_BUFFER_SIZE);
		pl.start("Skipping records...");
		wr.resetRead();
		for (int i = 0; i < numPages; i++) {
			wr.skip(in);
			pl.update();
		}
		pl.done();
		in.close();
	}

}


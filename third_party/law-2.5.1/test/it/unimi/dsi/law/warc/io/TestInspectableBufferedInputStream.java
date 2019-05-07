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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.poi.util.IOUtils;
import org.junit.Test;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;



//RELEASE-STATUS: DIST


/** A class to test {@link InspectableBufferedInputStream}. */

@SuppressWarnings("boxing")
public class TestInspectableBufferedInputStream {

	private static final Random r = new Random(0);

	/** A byte array input stream that will return its data in small chunks,
	 * even it could actually return more data.
	 */

	public static class BastardByteArrayInputStream extends FastByteArrayInputStream {
		private final static int BASTARD_LIMIT = 42;

		public BastardByteArrayInputStream(byte[] array) {
			super(array);
		}

		@Override
		public int read(byte[] buffer, int offset, int length) {
			return super.read(buffer, offset, length < BASTARD_LIMIT ? length : BASTARD_LIMIT);
		}

	}

	public static List<byte[]> byteArrays;
	static {
		byteArrays = new ArrayList<byte[]>();
		byte[] b;
		// Now generates byte buffers from 1 byte up to 64KiB; we shuffle them so that they are not increasing in size...
		for (int k = 0; k < 10; k++) {
			b = new byte[1 << k];
			r.nextBytes(b);
			byteArrays.add(b);
		}
		for (int k = 16; k >= 10; k--) {
			b = new byte[1 << k];
			r.nextBytes(b);
			byteArrays.add(b);
		}
		byteArrays.add(new byte[] {});
		byteArrays.add("This is a short\nnon empty and purely ASCII\nbyte sequence".getBytes());
	}


	@Test
	public void testSequentialRead() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1024); // Use 1KiB buffer
		for (byte[] byteArray: byteArrays) {
			System.out.printf("TESTING SEQUENTIAL READ FOR SIZE %d\n", byteArray.length);
			is.connect(new BastardByteArrayInputStream(byteArray));
			@SuppressWarnings("resource")
			BastardByteArrayInputStream bs = new BastardByteArrayInputStream(byteArray);
			int bbs;
			while ((bbs = bs.read()) != -1)
				assertEquals(bbs, is.read());
			assertEquals(is.read(), -1);
			is.close();
		}
	}

	@Test
	public void testReadFullyTotal() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1024); // Use 1KiB buffer
		for (byte[] byteArray: byteArrays) {
			System.out.printf("TESTING FULL READ FOR SIZE %d\n", byteArray.length);
			is.connect(new BastardByteArrayInputStream(byteArray));
			is.fillAndRewind();
			@SuppressWarnings("resource")
			BastardByteArrayInputStream bs = new BastardByteArrayInputStream(byteArray);
			int bbs;
			while ((bbs = bs.read()) != -1)
				assertEquals(bbs, is.read());
			assertEquals(is.read(), -1);
			assertEquals(is.readBytes(), byteArray.length);
			is.close();
		}
	}

	@Test
	public void testReadFullyPartial() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1024); // Use 1KiB buffer
		for (byte[] byteArray: byteArrays) {
			is.truncate(0);
			if (byteArray.length == 0) continue;
			int readJust = r.nextInt(byteArray.length);
			System.out.printf("TESTING FULL READ FOR SIZE %d, LIMITED AT %d\n", byteArray.length, readJust);
			is.connect(new BastardByteArrayInputStream(byteArray));
			is.fill(readJust);
			is.rewind();
			@SuppressWarnings("resource")
			BastardByteArrayInputStream bs = new BastardByteArrayInputStream(byteArray);
			int bbs;
			int read = 0;
			while (read < readJust && (bbs = bs.read()) != -1) {
				assertEquals(bbs, is.read());
				read++;
			}
			assertTrue(is.overflowLength() <= readJust);
			is.close();
		}
	}

	@Test
	public void testMultipleSequentialRead() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1024); // Use 1KiB buffer
		for (byte[] byteArray: byteArrays) {
			System.out.printf("TESTING MULTIPLE SEQUENTIAL READ FOR SIZE %d (>=total,<=partial): ", byteArray.length);
			is.connect(new BastardByteArrayInputStream(byteArray));

			// How many read
			int k = r.nextInt(10);
			for (int s = 0; s < k; s++) {
				is.rewind();
				@SuppressWarnings("resource")
				BastardByteArrayInputStream bs = new BastardByteArrayInputStream(byteArray);
				int bbs;
				if (r.nextDouble() < .5) {
					System.out.print(">");
					// Read to EOF
					while ((bbs = bs.read()) != -1)
						assertEquals(bbs, is.read());
					assertEquals(is.read(), -1);
				} else {
					// Read only partially
					int howManyBytes = r.nextInt(byteArray.length + 1);
					System.out.print("<");
					for (int t = 0; t < howManyBytes; t++)
						assertEquals(bs.read(), is.read());
				}
			}
			System.out.println();
			is.close();
		}
	}

	@Test
	public void testTruncate() throws IOException {
		// Creates a temporary directory
		File tempDir = File.createTempFile("mydir", null);
		tempDir.delete();
		tempDir.mkdir();
		//tempDir.deleteOnExit();
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1024, tempDir); // Use 1KiB buffer
		for (byte[] byteArray: byteArrays) {
			System.out.printf("TESTING truncate() FOR SIZE %d (+=truncation): ", byteArray.length);
			// Reads the is 100 bytes at a time up to (almost) its end...
			is.connect(new BastardByteArrayInputStream(byteArray));
			@SuppressWarnings("resource")
			BastardByteArrayInputStream bs = new BastardByteArrayInputStream(byteArray);
			byte[] bis = new byte[100];
			byte[] bbs = new byte[100];
			int readFromIs, readFromBis;
			while ((readFromIs = is.read(bis)) > 0) {
				int from = 0, howManyLeft = readFromIs;
				do {
					readFromBis = bs.read(bbs, from, howManyLeft);
					from += readFromBis;
					howManyLeft -= readFromBis;
				} while (howManyLeft > 0);
				for (int i = 0; i < readFromIs; i++)
					assertEquals(bbs[i], bis[i]);
			}
			is.close();
			//if (1 > 0) return;
			File t = is.overflowFile;
			if (r.nextDouble() < .9) {
				long newSize = r.nextInt(1 + (int)(t.length() * 2));
				System.out.print("+");
				is.truncate(newSize);
				assertEquals(tempDir.listFiles().length, 1); // There should be just one single temporary file here
				System.out.println("t.length()=" + t.length() + ", newSize=" + newSize);
				assertTrue(t.length() <= newSize);
				System.out.printf("(%d<=%d)", t.length(), newSize);
			}
			System.out.println();
		}
	}

	@Test
	public void testDirectInspection() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1024); // Use 1KiB buffer
		for (byte[] byteArray: byteArrays) {
			System.out.printf("TESTING DIRECT INSPECTION FOR SIZE %d\n", byteArray.length);
			is.connect(new BastardByteArrayInputStream(byteArray));
			// Read some random number of bytes
			int toBeRead = r.nextInt(byteArray.length + 1), read, from;
			byte[] bis = new byte[toBeRead];
			from = 0;
			do {
				read = is.read(bis, from, toBeRead);
				if (read < 0) break;
				from += read;
				toBeRead -= read;
			} while (toBeRead > 0);
			// Test that the first read bytes are ok
			for (int c = 0; c < toBeRead; c++) assertEquals(bis[c], byteArray[c]);
			// Now test within the available bytes
			for (int c = 0; c < is.inspectable; c++) assertEquals(is.buffer[c], byteArray[c]);
		}

		is.close();
	}

	@Test
	public void testReadBulk() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1024); // Use 1KiB buffer
		for (byte[] byteArray: byteArrays) {
			System.out.printf("TESTING READ BULK FOR SIZE %d (+=read,R=rewind): ", byteArray.length);
			is.connect(new BastardByteArrayInputStream(byteArray));
			FastByteArrayInputStream bs = new FastByteArrayInputStream(byteArray);
			// Decide how many reads
			int reads = r.nextInt(5);
			for (int t = 0; t < reads; t++) {
				System.out.print("+");
				byte[] bis = new byte[r.nextInt(1 + byteArray.length * 3 / 2)];
				byte[] bbs = new byte[bis.length];
				int offset = bis.length < 2? 0 : r.nextInt(bis.length / 2);
				int length = bis.length - offset == 0? 0 : r.nextInt(bis.length - offset);
				int res1 = IOUtils.readFully(is, bis, offset, length);
				int res2 = IOUtils.readFully(bs, bbs, offset, length);
				if (r.nextDouble() < .1) {
					// In the 10% of attempts we rewind
					System.out.print("R");
					is.rewind();
					bs = new BastardByteArrayInputStream(byteArray);
				}
				assertEquals(res1, res2);
				for (int i = 0; i < Math.max(res1, 0); i++) {
					assertEquals(bis[offset + i], bbs[offset + i]);
				}
			}
			System.out.println();
		}
	}

	@Test
	public void testLength() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1024); // Use 1KiB buffer
		for (byte[] byteArray: byteArrays) {
			if (byteArray.length == 0) continue; // skip size 0
			System.out.printf("TESTING LENGTH FOR SIZE %d", byteArray.length);
			is.connect(new BastardByteArrayInputStream(byteArray));
			// Choose whether to perform some read or not
			if (r.nextBoolean())
				// Choose whether to read it completely or not
				if (r.nextBoolean()) {
					is.fill(Long.MAX_VALUE);
					System.out.printf(", filled\n");
				}
				else {
					int toBeRead = r.nextInt(byteArray.length * 2 + 1), i;
					for (i = 0; i < toBeRead; i++)
						if (is.read() < 0) break;
					System.out.printf(", read %d bytes%s\n", i, i < toBeRead? "" : " (up to eof)");
				}
			else System.out.print(", no read\n");
			assertEquals(byteArray.length, is.length());
		}
		is.close();
	}

	@Test
	public void testFillAndRewind() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1000); // Use 1KiB buffer
		is.connect(new ByteArrayInputStream(new byte[3000]));
		is.fill(2000);
		is.rewind();
		final long length = is.length();
		while(is.read() != -1);
		assertEquals(length, is.length());
		is.close();
	}

	@Test
	public void testLengthDoesNotAlterState() throws IOException {
		InspectableBufferedInputStream is = new InspectableBufferedInputStream(1000); // Use 1KiB buffer
		is.connect(new ByteArrayInputStream(new byte[3000]));
		is.fill(2000);
		is.rewind();
		is.length();
		assertEquals(0, is.position());
		is.close();
	}


}

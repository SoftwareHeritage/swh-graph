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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.poi.util.IOUtils;
import org.junit.Test;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;



//RELEASE-STATUS: DIST


/** A class to test {@link MeasurableSequenceInputStream}. */

public class TestMeasurableSequenceInputStream {

	private static final Random r = new Random(0);

	private static List<byte[]> byteArrays;
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
		for (byte[] byteArray: byteArrays) {
			System.out.println("TESTING SEQUENTIAL, ONE INPUTSTREAM, READ FOR SIZE " + byteArray.length);
			MeasurableSequenceInputStream is = new MeasurableSequenceInputStream(new FastByteArrayInputStream(byteArray));
			FastByteArrayInputStream bs = new FastByteArrayInputStream(byteArray);
			int bbs;
			while ((bbs = bs.read()) != -1) {
				assertEquals(bbs, is.read());
				assertEquals(bs.position(), is.position());
			}
			assertEquals(is.read(), -1);
			is.close();
			bs.close();
			assertEquals(byteArray.length, is.position());
		}
	}

	@Test
	public void testSequentialSequenceRead() throws IOException {
		for (byte[] byteArray: byteArrays) {
			System.out.println("TESTING SEQUENTIAL, TWO INPUTSTREAMS, READ FOR SIZE " + byteArray.length);
			MeasurableSequenceInputStream is = new MeasurableSequenceInputStream(null, new FastByteArrayInputStream(byteArray), null, new FastByteArrayInputStream(byteArray), null);
			byte[] doubleByteArray = new byte[2 * byteArray.length];
			System.arraycopy(byteArray, 0, doubleByteArray, 0, byteArray.length);
			System.arraycopy(byteArray, 0, doubleByteArray, byteArray.length, byteArray.length);
			FastByteArrayInputStream bs = new FastByteArrayInputStream(doubleByteArray);
			int bbs;
			while ((bbs = bs.read()) != -1) {
				assertEquals(bbs, is.read());
				assertEquals(bs.position(), is.position());
			}
			assertEquals(is.read(), -1);
			is.close();
			bs.close();
			assertEquals(doubleByteArray.length, is.position());
		}
	}

	@Test
	public void testReadBulk() throws IOException {
		for (byte[] byteArray: byteArrays) {
			System.out.println("TESTING READ BULK, ONE INPUTSTREAM, FOR SIZE " + byteArray.length);
			MeasurableSequenceInputStream is = new MeasurableSequenceInputStream(new FastByteArrayInputStream(byteArray));
			FastByteArrayInputStream bs = new FastByteArrayInputStream(byteArray);
			// Decide how many reads
			int reads = r.nextInt(5);
			for (int t = 0; t < reads; t++) {
				byte[] bis = new byte[r.nextInt(1 + byteArray.length * 3 / 2)];
				byte[] bbs = new byte[bis.length];
				int offset = bis.length < 2 ? 0 : r.nextInt(bis.length / 2);
				int length = bis.length - offset == 0? 0 : r.nextInt(bis.length - offset);
				int res1 = IOUtils.readFully(is, bis, offset, length);
				int res2 = IOUtils.readFully(bs, bbs, offset, length);
				assertEquals(res1, res2);
				assertEquals(bs.position(), is.position());
				for (int i = 0; i < Math.max(res1, 0); i++) {
					assertEquals(bis[offset + i], bbs[offset + i]);
				}
			}
		}
	}

	@Test
	public void testReadSequentialBulk() throws IOException {
		for (byte[] byteArray: byteArrays) {
			System.out.println("TESTING READ BULK, TWO INPUTSTREAMS, FOR SIZE " + byteArray.length);
			MeasurableSequenceInputStream is = new MeasurableSequenceInputStream(null, new FastByteArrayInputStream(byteArray), null, new FastByteArrayInputStream(byteArray), null);
			byte[] doubleByteArray = new byte[2 * byteArray.length];
			System.arraycopy(byteArray, 0, doubleByteArray, 0, byteArray.length);
			System.arraycopy(byteArray, 0, doubleByteArray, byteArray.length, byteArray.length);
			FastByteArrayInputStream bs = new FastByteArrayInputStream(doubleByteArray);
			// Decide how many reads
			int reads = r.nextInt(5);
			for (int t = 0; t < reads; t++) {
				byte[] bis = new byte[r.nextInt(1 + doubleByteArray.length * 3 / 2)];
				byte[] bbs = new byte[bis.length];
				int offset = bis.length < 2 ? 0 : r.nextInt(bis.length / 2);
				int length = bis.length - offset == 0? 0 : r.nextInt(bis.length - offset);
				int res1 = IOUtils.readFully(is, bis, offset, length);
				int res2 = IOUtils.readFully(bs, bbs, offset, length);
				assertEquals(res1, res2);
				assertEquals(bs.position(), is.position());
				for (int i = 0; i < Math.max(res1, 0); i++) {
					assertEquals(bis[offset + i], bbs[offset + i]);
				}
			}
		}
	}

}

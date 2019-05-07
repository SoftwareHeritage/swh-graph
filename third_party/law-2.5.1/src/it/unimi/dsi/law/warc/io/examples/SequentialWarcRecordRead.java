package it.unimi.dsi.law.warc.io.examples;

import java.io.File;
import java.io.FileInputStream;

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

// RELEASE-STATUS: DIST

public class SequentialWarcRecordRead {

	final static int IO_BUFFER_SIZE = 64 * 1024;

	public static void main(String arg[]) throws Exception {

		final String warcFile = "test";
		final boolean isGZipped = true;
		final WarcRecord record = isGZipped ? new GZWarcRecord() : new WarcRecord();

		final FastBufferedInputStream in = new FastBufferedInputStream(
				new FileInputStream(new File(warcFile + ".warc" + (isGZipped ? ".gz" : ""))), IO_BUFFER_SIZE);

		for (;;) {

			if (record.read(in) == -1) break;
			if (isGZipped) System.out.println("GZip header:\n" + ((GZWarcRecord)record).gzheader);
			System.out.println("WARC header:\n" + record.header);
			System.out.println("First ten bytes of block:");

			int n = 10, r;
			while ((r = record.block.read()) != -1 && n-- > 0)
				System.out.print(Integer.toHexString(r) + " ");

			System.out.println("\n");

		}

	}
}

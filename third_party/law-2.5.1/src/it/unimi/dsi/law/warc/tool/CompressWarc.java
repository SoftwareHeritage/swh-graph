package it.unimi.dsi.law.warc.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

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
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.law.warc.io.GZWarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.stat.SummaryStats;

// RELEASE-STATUS: DIST

/** A tool to compress a WARC file. */

public class CompressWarc {
	private final static Logger LOGGER = LoggerFactory.getLogger(CompressWarc.class);

	/**
	 * This method reads from a given input stream a sequence of uncompressed
	 * WARC records and writes to a given output stream a compressed version of
	 * them.
	 *
	 * @param in the input stream.
	 * @param out the output stream.
	 * @throws IOException
	 * @throws FormatException
	 */
	public static void run(final FastBufferedInputStream in, final OutputStream out) throws IOException, FormatException {
		final WarcRecord inRecord = new WarcRecord();
		final GZWarcRecord outRecord = new GZWarcRecord();

		final SummaryStats compressionRatio = new SummaryStats();

		final ProgressLogger pl = new ProgressLogger(LOGGER, "records");
		pl.logInterval = ProgressLogger.TEN_SECONDS;
		pl.info = new Object() {
			@Override
			public String toString() {
				final long size = compressionRatio.size64();
				return "compression ratio: " + (size != 0 ? (int)(100 * compressionRatio.sum() / size) + "%" : "NA");
			}
		};

		pl.start("Compressing...");
		while (inRecord.read(in) != -1) {
			outRecord.copy(inRecord);
			outRecord.write(out);
			compressionRatio.add((double)outRecord.gzheader.compressedSkipLength / inRecord.header.dataLength);
			pl.update();
		}
		pl.done();
	}

	final static int IO_BUFFER_SIZE = 64 * 1024;

	public static void main(String arg[]) throws Exception {
		SimpleJSAP jsap = new SimpleJSAP(CompressWarc.class.getName(), "Compress a warc file.",
				new Parameter[] {
			new UnflaggedOption("warcFile", JSAP.STRING_PARSER, "-", JSAP.REQUIRED,	JSAP.NOT_GREEDY, "The Warc file basename (if not present, or -, stdin/stdout will be used).")
			});

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted())	return;

		final String warcFile = jsapResult.getString("warcFile");

		final FastBufferedInputStream in = new FastBufferedInputStream(warcFile.equals("-") ? System.in : new FileInputStream(new File(	warcFile + ".warc")), IO_BUFFER_SIZE);
		final FastBufferedOutputStream out = new FastBufferedOutputStream(warcFile.equals("-") ? System.out : new FileOutputStream(new File(warcFile + ".warc.gz")), IO_BUFFER_SIZE);

		run(in, out);

		in.close();
		out.close();
	}
}

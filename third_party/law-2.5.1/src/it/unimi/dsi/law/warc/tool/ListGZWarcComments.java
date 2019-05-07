package it.unimi.dsi.law.warc.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
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
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;
import it.unimi.dsi.law.warc.util.Util;
import it.unimi.dsi.logging.ProgressLogger;

// RELEASE-STATUS: DIST

/** A tool to list the GZip header comments contained in a compressed WARC file. */

public class ListGZWarcComments {
	private final static Logger LOGGER = LoggerFactory.getLogger(ListGZWarcComments.class);

	/**
	 * Writes on the given writer the GZip header comment filed.
	 *
	 * @param in the input stream.
	 * @param pw the writer.
	 * @throws IOException
	 * @throws FormatException
	 */
	public static void run(final FastBufferedInputStream in, final PrintWriter pw) throws IOException, FormatException {
		final GZWarcRecord record = new GZWarcRecord();
		final ProgressLogger pl = new ProgressLogger(LOGGER, 1, TimeUnit.MINUTES, "records");

		pl.start("Listing...");
		while (record.skip(in) != -1) { // we just need headers
			pw.println(Util.getString(record.gzheader.comment));
			pl.update();
		}
		pl.done();
	}

	public static final String DEFAULT_BUFFER_SIZE = "64Ki";

	public static void main(String arg[]) throws Exception {
		SimpleJSAP jsap = new SimpleJSAP(ListGZWarcComments.class.getName(), "Lists the gzip comments of a compressed warc file.",
				new Parameter[] {
			new FlaggedOption("bufferSize", JSAP.INTSIZE_PARSER, DEFAULT_BUFFER_SIZE, JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer."),
			new UnflaggedOption("warcFile", JSAP.STRING_PARSER, null, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The Warc input file basename (if not present, or -, stdin will be used)."),
		});
		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final String warcFile = jsapResult.getString("warcFile");
		final int bufferSize = jsapResult.getInt("bufferSize");

		final FastBufferedInputStream in = new FastBufferedInputStream(warcFile.equals("-") ? System.in : new FileInputStream(new File(warcFile + ".warc.gz")), bufferSize);
		final PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FastBufferedOutputStream(System.out, bufferSize), "ASCII"));

		try {
			run(in, pw);
		}
		finally {
			pw.close();
			in.close();
		}
	}
}

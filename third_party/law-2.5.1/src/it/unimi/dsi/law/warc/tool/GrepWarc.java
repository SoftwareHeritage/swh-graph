package it.unimi.dsi.law.warc.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
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
import it.unimi.dsi.fastutil.objects.ObjectRBTreeSet;
import it.unimi.dsi.law.warc.filters.AbstractFilter;
import it.unimi.dsi.law.warc.filters.Filter;
import it.unimi.dsi.law.warc.filters.Filters;
import it.unimi.dsi.law.warc.filters.parser.FilterParser;
import it.unimi.dsi.law.warc.io.GZWarcRecord;
import it.unimi.dsi.law.warc.io.WarcFilteredIterator;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.logging.ProgressLogger;

// RELEASE-STATUS: DIST

/** A "grep" for WARC files. */

public class GrepWarc {
	private final static Logger LOGGER = LoggerFactory.getLogger(GrepWarc.class);

	/**
	 * This method acts as a sort of "grep" for WARC files.
	 *
	 * <p> It reads from a given input stream a sequence of (possibly compressed)
	 * WARC records, and writes the one that are accepted by the specified
	 * {@link AbstractFilter} to a given output stream (uncompressed).
	 *
	 * @param in the input stream.
	 * @param isGZipped tells if the input stream contains compressed WARC records.
	 * @param filter the filter.
	 * @param out the output stream.
	 * @throws IOException
	 */
	public static void run(final FastBufferedInputStream in, final boolean isGZipped, final Filter<WarcRecord> filter, final OutputStream out) throws IOException {
		final WarcRecord inRecord = isGZipped ? new GZWarcRecord() : new WarcRecord();
		final WarcRecord outRecord = new WarcRecord();
		final ProgressLogger pl = new ProgressLogger(LOGGER, "records");
		final WarcFilteredIterator it = new WarcFilteredIterator(in, inRecord, filter, pl);

		pl.logInterval = ProgressLogger.TEN_SECONDS;
		pl.start("Grepping...");
		while (it.hasNext()) {
			it.next(); // this will update pl
			outRecord.copy(inRecord);
			outRecord.write(out);
		}
		pl.done();
	}

	final static int IO_BUFFER_SIZE = 64 * 1024;

	public static void main(String arg[]) throws Exception {
		SortedSet<String> filterNames = new ObjectRBTreeSet<String>();

		for(Class<? extends Filter<?>> c : Filters.standardFilters()) filterNames.add(c.getSimpleName());

		SimpleJSAP jsap = new SimpleJSAP(GrepWarc.class.getName(), "Grep for warc files.",
				new Parameter[] {
			new Switch("gzip", 'z', "gzip", "Tells if the warc is compressed."),
			new UnflaggedOption("filter", JSAP.STRING_PARSER, JSAP.REQUIRED, "The filter. " + filterNames),
			new UnflaggedOption("warcFile", JSAP.STRING_PARSER, "-", JSAP.REQUIRED, JSAP.NOT_GREEDY, "The Warc input file basename (if not present, or -, stdin will be used)."),
		});
		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean isGZipped = jsapResult.getBoolean("gzip");
		final Filter<WarcRecord> filter = new FilterParser<WarcRecord>(WarcRecord.class).parse(jsapResult.getString("filter"));
		final String warcFile = jsapResult.getString("warcFile");

		final FastBufferedInputStream in = new FastBufferedInputStream(warcFile.equals("-") ? System.in : new FileInputStream(new File(warcFile + ".warc" + (isGZipped ? ".gz" : ""))), IO_BUFFER_SIZE);
		final FastBufferedOutputStream out = new FastBufferedOutputStream(System.out, IO_BUFFER_SIZE);

		run(in, isGZipped, filter, out);

		in.close();
		out.close();
	}
}

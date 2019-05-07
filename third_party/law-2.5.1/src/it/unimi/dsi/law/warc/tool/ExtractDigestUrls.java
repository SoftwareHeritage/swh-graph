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
import it.unimi.dsi.law.warc.filters.Filter;
import it.unimi.dsi.law.warc.filters.parser.FilterParser;
import it.unimi.dsi.law.warc.io.GZWarcRecord;
import it.unimi.dsi.law.warc.io.HttpResponseFilteredIterator;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.util.HttpResponse;
import it.unimi.dsi.law.warc.util.Util;
import it.unimi.dsi.law.warc.util.WarcHttpResponse;
import it.unimi.dsi.logging.ProgressLogger;

// RELEASE-STATUS: DIST

/** A tool to extract digests and URLs from response records of a WARC file. */

public class ExtractDigestUrls {
	private final static Logger LOGGER = LoggerFactory.getLogger(ExtractDigestUrls.class);

	public static void run(final FastBufferedInputStream in, final boolean isGZipped, final Filter<HttpResponse> filter, final PrintWriter pw) throws IOException {
		final WarcRecord record = isGZipped ? new GZWarcRecord() : new WarcRecord();
		final ProgressLogger pl = new ProgressLogger(LOGGER, 1, TimeUnit.MINUTES, "records");
		final WarcHttpResponse response = new WarcHttpResponse();
		final HttpResponseFilteredIterator it = new HttpResponseFilteredIterator(in, record, response, filter);

		pl.start("Listing...");
		long pos = -1;
		WarcRecord.Header header = null;
		try{
			while (it.hasNext()) {
				// ALERT: meaningless position detection?
				pos = in.position();
				it.next();
				header = record.header;
				String digest = response.digest() != null ? Util.toHexString(response.digest()) : header.recordId.toString();
				pw.println(digest + "\t" + header.subjectUri + "\t" + (response.isDuplicate() ? "D" : "N"));
				pl.update();
			}
		} catch (RuntimeException e) {
			System.err.println("Got " + e);
			System.err.println("Position: " + pos + ", last url header:\n" + header);
			throw e;
		}
		pl.done();
	}

	public static final String DEFAULT_BUFFER_SIZE = "64Ki";

	public static void main(String arg[]) throws Exception {
		SimpleJSAP jsap = new SimpleJSAP(ExtractDigestUrls.class.getName(), "Extracts digests and URLs from http response records of a WARC file.",
				new Parameter[] {
			new Switch("gzip", 'z', "gzip", "Whether the Warc file is compressed."),
			new FlaggedOption("filter", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "filter", "The filter."),
			new FlaggedOption("bufferSize", JSAP.INTSIZE_PARSER, DEFAULT_BUFFER_SIZE, JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer."),
			new UnflaggedOption("warcFile", JSAP.STRING_PARSER, "-", JSAP.REQUIRED, JSAP.NOT_GREEDY, "The Warc input file basename (if not present, or -, stdin will be used)."),
		});
		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean isGZipped = jsapResult.getBoolean("gzip");
		final String filterSting = jsapResult.getString("filter") == null ? "TRUE" : jsapResult.getString("filter");
		final int bufferSize = jsapResult.getInt("bufferSize");
		final String warcFile = jsapResult.getString("warcFile");

		final Filter<HttpResponse> filter = new FilterParser<HttpResponse>(HttpResponse.class).parse(filterSting);

		final FastBufferedInputStream in = new FastBufferedInputStream(warcFile.equals("-") ? System.in : new FileInputStream(new File(warcFile + ".warc" + (isGZipped ? ".gz" : ""))), bufferSize);
		final PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FastBufferedOutputStream(System.out, bufferSize), "ASCII"));

		run(in, isGZipped, filter, pw);

		in.close();
		pw.close();
	}
}

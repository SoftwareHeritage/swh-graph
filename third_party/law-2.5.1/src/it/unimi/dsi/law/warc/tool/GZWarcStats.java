package it.unimi.dsi.law.warc.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

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
import it.unimi.dsi.law.warc.io.GZWarcRecord;
import it.unimi.dsi.law.warc.io.GZWarcRecord.GZHeader;
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.stat.SummaryStats;

// RELEASE-STATUS: DIST

/** A tool to compute some statistics about a gzipped WARC file. */

public class GZWarcStats {
	private final static Logger LOGGER = LoggerFactory.getLogger(GZWarcStats.class);

	public static long run(final FastBufferedInputStream in, final SummaryStats uncompressedSize, final SummaryStats compressedSize, final SummaryStats compressionRatio) throws IOException, FormatException {
		final GZWarcRecord r = new GZWarcRecord();
		final ProgressLogger pl = new ProgressLogger(LOGGER, "records");
		pl.logInterval = ProgressLogger.TEN_SECONDS;
		pl.start("Analyzing...");
		while (r.read(in) != -1) {
			final GZHeader gzheader = r.gzheader;
			compressedSize.add(gzheader.compressedSkipLength);
			uncompressedSize.add(gzheader.uncompressedSkipLength);
			compressionRatio.add((int)(100 * (double)gzheader.compressedSkipLength / gzheader.uncompressedSkipLength));
			pl.update();
		}
		pl.done();
		return pl.count;
	}

	final static int IO_BUFFER_SIZE = 64 * 1024;

	public static void main(String arg[]) throws Exception {
		SimpleJSAP jsap = new SimpleJSAP(GZWarcStats.class.getName(), "Compute some statistics about a gzipped warc file.",
				new Parameter[] {
					new Switch("html", 'h', "html", "Generate output in HTML format."),
					new Switch("headers", 'H', "header", "Generate HTML table headers format."),
					new UnflaggedOption("warcFile", JSAP.STRING_PARSER, "-", JSAP.REQUIRED, JSAP.NOT_GREEDY, "The gzipped Warc file basename (if not present, or -, stdin/stdout will be used).")
			});

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted())	return;

		final String warcFile = jsapResult.getString("warcFile");
		final boolean html = jsapResult.getBoolean("html");
		final boolean headers = jsapResult.getBoolean("headers");

		final SummaryStats uncompressedSize = new SummaryStats();
		final SummaryStats compressedSize = new SummaryStats();
		final SummaryStats compressionRatio = new SummaryStats();

		final FastBufferedInputStream in = new FastBufferedInputStream(warcFile.equals("-") ? System.in : new FileInputStream(new File(	warcFile + ".warc.gz")), IO_BUFFER_SIZE);
		final long n = run(in, uncompressedSize, compressedSize, compressionRatio);
		in.close();

		if (html) {

			if (headers) {
				System.out.println("<TABLE border='1'>");
				System.out.println("<TR><TH rowspan='2'>Name<TH rowspan='2'>Num.<br>Records<TH colspan='5'>Compressed byte size<TH colspan='5'>Uncompressed byte size<TH colspan='5'>Compression ratio (%)");
				System.out.println("<TR><TH>min<TH>max<TH>average<TH>stdev<TH>sum<TH>min<TH>max<TH>average<TH>stdev<TH>sum<TH>min<TH>max<TH>average<TH>stdev");
			}

			System.out.print("<tr><td>" + warcFile + "<td>" + n);
			System.out.print("<td>" + (long)compressedSize.min()
					+ "<td>" + (long)compressedSize.max()
					+ "<td>" + (int)(100 * compressedSize.mean()) / 100.0
					+ "<td>" + (int)(100 * compressedSize.standardDeviation()) / 100.0
					+ "<td>" + (long)compressedSize.sum());
			System.out.print("<td>" + (long)uncompressedSize.min()
					+ "<td>" + (long)uncompressedSize.max()
					+ "<td>" + (int)(100 * uncompressedSize.mean()) / 100.0
					+ "<td>" + (int)(100 * uncompressedSize.standardDeviation()) / 100.0
					+ "<td>" + (long)uncompressedSize.sum());
			System.out.print("<td>" + (long)compressionRatio.min()
					+ "<td>" + (long)compressionRatio.max()
					+ "<td>" + (int)(100 * compressionRatio.mean()) / 100.0
					+ "<td>" + (int)(100 * compressionRatio.standardDeviation()) / 100.0);
			System.out.println();

			if (headers) System.out.println("</TABLE>");

		} else {

			System.out.println("Records: " + n);
			System.out.println("Compressed size: min = " + (long)compressedSize.min()
					+ ", max = " + (long)compressedSize.max()
					+ ", avg = " + compressedSize.mean()
					+ ", sd = " + compressedSize.standardDeviation()
					+ ", sum = " + (long)compressedSize.sum());
			System.out.println("Uncompressed size: min = " + (long)uncompressedSize.min()
					+ ", max = " + (long)uncompressedSize.max()
					+ ", avg = " + uncompressedSize.mean()
					+ ", sd = " + uncompressedSize.standardDeviation()
					+ ", sum = " + (long)uncompressedSize.sum());
			System.out.println("Compression ratio: min = " + (long)compressionRatio.min()
					+ "%, max = " + (long)compressionRatio.max()
					+ "%, avg = " + compressionRatio.mean()
					+ "%, sd = " + compressionRatio.standardDeviation() +"%");

		}
	}
}

package it.unimi.dsi.law.warc.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

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

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.law.warc.io.GZWarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.io.WarcRecord.FormatException;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.StringMap;

// RELEASE-STATUS: DIST

/** A class to extract specific records from a WARC file. */

public class CutWarc {
	private final static Logger LOGGER = LoggerFactory.getLogger(CutWarc.class);

	public static void run(final FastBufferedInputStream warc, final RandomAccessFile idx, final boolean isGZippedInput, final boolean isGZippedOutput, long[] record, int recordCount, final OutputStream out) throws IOException, FormatException {
		final WarcRecord inRecord = isGZippedInput ? new GZWarcRecord() : new WarcRecord();
		final WarcRecord outRecord = isGZippedOutput ? new GZWarcRecord() : new WarcRecord();

		final ProgressLogger logger = new ProgressLogger(LOGGER, "documents");

		logger.start("Cutting documents");
		for (int i = 0; i < recordCount; i++)
		{
			idx.seek(record[i] * 8);
			final long pos = idx.readLong();
			warc.position(pos);
			inRecord.resetRead();
			inRecord.read(warc);
			outRecord.copy(inRecord);
			outRecord.write(out);
			logger.lightUpdate();
		}

		logger.stop();
	}

	final static int IO_BUFFER_SIZE = 64 * 1024;

	@SuppressWarnings("unchecked")
	public static void main(String arg[]) throws Exception {
		SimpleJSAP jsap = new SimpleJSAP(CutWarc.class.getName(), "Cuts (that is, extracts record) from a warc file. It requires an index.",
				new Parameter[] {
			new Switch("gzip", 'z', "gzip", "Tells if the input warc is compressed."),
			new Switch("outzip", 'Z', "outzip", "Tells if the output warc must be compressed."),
			new Switch("permissive", 'p', "permissive", "Ignore unknown urls instead of throwing an exception"),
			new FlaggedOption("recordFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "recordFile", "A file containing, one per line, the ordinal numbers or URL of records to be output."),
			new FlaggedOption("urlMap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'm', "url-map", "The term map from URL to record number."),
			new UnflaggedOption("warcFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The Warc file basename."),
			new UnflaggedOption("recordSpec", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.GREEDY , "The spec (ordinal number or URL) of records to be output."),
			});

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		CharSequence[] recordSpec = null;
		if (!jsapResult.userSpecified("recordFile") && !jsapResult.userSpecified("recordSpec"))
			throw new IllegalArgumentException("One of the two options recordFile and recordSpec must be set.");
		if (jsapResult.userSpecified("recordSpec") && jsapResult.userSpecified("recordFile"))
			throw new IllegalArgumentException("You cannot specify both recordFile and recordSpec options");

		if (jsapResult.userSpecified("recordSpec"))
			recordSpec = jsapResult.getStringArray("recordSpec");
		else
			// TODO Variable charset spec
			recordSpec = new FileLinesCollection (jsapResult.getString ("recordFile"), "UTF-8").allLines().toArray (new CharSequence[0]) ;

		final String warcFile = jsapResult.getString("warcFile");
		final boolean isGZippedInput = jsapResult.getBoolean("gzip");
		final boolean isGZippedOutput = jsapResult.getBoolean ("outzip");
		final boolean bePermissive = jsapResult.getBoolean ("permissive");

		final long[] record = new long[recordSpec.length];
		final StringMap<? extends CharSequence> map = jsapResult.getString("urlMap") == null? null : (StringMap<? extends CharSequence>)BinIO.loadObject(jsapResult.getString("urlMap"));

		int recordCount = 0;

		for (int i = 0; i < recordSpec.length;i ++) {
			try {
				record[recordCount] = Long.parseLong(recordSpec[i].toString());
				if (record [recordCount] >= 0) recordCount++; // Skip non-existing urls.

			} catch (NumberFormatException e) {
				if (map == null) throw new RuntimeException("URLs cannot be specified if a map is not provided");
				record[recordCount] = map.getLong(recordSpec[i]);
				if (record[recordCount] < 0)
				{
					if (!bePermissive)
						throw new RuntimeException("URL " + recordSpec[i] + " cannot be resolved");
				} else
					recordCount++;
			}
		}


		// Now sort records  for better file access .
		LongArrays.quickSort(record, 0, recordCount);


		final FastBufferedInputStream warc = new FastBufferedInputStream(new FileInputStream(new File(warcFile + ".warc" + (isGZippedInput ? ".gz" : ""))), IO_BUFFER_SIZE);
		final RandomAccessFile idx = new RandomAccessFile(new File(warcFile + ".warc" + (isGZippedInput ? ".gz" : "") + ".idx"), "r");
		final FastBufferedOutputStream out = new FastBufferedOutputStream(System.out, IO_BUFFER_SIZE);

		run(warc, idx, isGZippedInput, isGZippedOutput, record, recordCount, out);

		warc.close();
		idx.close();
		out.close();
	}
}

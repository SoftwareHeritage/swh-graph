package it.unimi.dsi.law.io.tool;

/*
 * Copyright (C) 2004-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.law.util.CRC64;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.StringMap;

// RELEASE-STATUS: DIST

/**
 * The main method of this class reads a UTF-8 file containg a newline separated
 * list of strings and writes a {@link java.io.DataOutputStream} containing a
 * list of ints such that the <var>i</var>-th int is equal to the <var>j</var>-th
 * int iff the ({@linkplain it.unimi.dsi.law.util.CRC64 crc} of the) <var>i</var>-th
 * string is equal to the ({@linkplain it.unimi.dsi.law.util.CRC64 crc} of
 * the) <var>j</var>-th string. The minimum int will be 0 and the maximum int
 * will be equal to the number of different strings minus one.
 */

public class NumberDistinctLines {
	private final static Logger LOGGER = LoggerFactory.getLogger(NumberDistinctLines.class);

	private final static int BUFFER_SIZE = 1024 * 1024;

	private NumberDistinctLines() {}

	@SuppressWarnings("unchecked")
	public static void main(final String arg[]) throws IOException, JSAPException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(NumberDistinctLines.class.getName(), "Numeber distinct lines.",
				new Parameter[] {
					new FlaggedOption("restrictFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'r', "restrict", "The SignedMinimalPerfectHash of the only strings to be considered."),
					new UnflaggedOption("stringsFile", JSAP.STRING_PARSER, JSAP.REQUIRED, "The intput file of UTF-8 strings to number."),
					new UnflaggedOption("intsFile", JSAP.STRING_PARSER, JSAP.REQUIRED, "The output file of ints numbering distinct lines.")
			});
		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final Long2IntOpenHashMap crc2int = new Long2IntOpenHashMap();
		crc2int.defaultReturnValue(-1);

		final String inFileName = jsapResult.getString("stringsFile"), outFileName = jsapResult.getString("intsFile");

		final FastBufferedReader in = new FastBufferedReader(new InputStreamReader(inFileName.equals("-") ? System.in : new FileInputStream(inFileName), "UTF-8"),  BUFFER_SIZE);
		final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outFileName.equals("-") ? (OutputStream)System.out : new FileOutputStream(outFileName), BUFFER_SIZE));

		final StringMap<? extends CharSequence> restrict = jsapResult.contains("restrictFile") ? (StringMap<? extends CharSequence>)BinIO.loadObject(jsapResult.getString("restrictFile")) : null;

		final ProgressLogger pm = new ProgressLogger(LOGGER, "lines");
		pm.start("Converting...");

		final MutableString s = new MutableString();
		int numStrings = 0;
		while ((in.readLine(s)) != null) {
			if (restrict != null && restrict.getLong(s) == -1) {
				out.writeInt(-1);
			} else {
				int i;
				final long crc = CRC64.compute(s);
				if ((i = crc2int.get(crc)) == -1) crc2int.put(crc, i = numStrings++);
				out.writeInt(i);
			}
			pm.update();
		}

		in.close();
		out.close();

		pm.stop("Done. Number of different strings " + numStrings  + " (map size " + crc2int.size() + ").");

	}
}


package it.unimi.dsi.law.warc.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URI;
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

import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.law.warc.filters.Filter;
import it.unimi.dsi.law.warc.filters.parser.FilterParser;
import it.unimi.dsi.law.warc.io.GZWarcRecord;
import it.unimi.dsi.law.warc.io.HttpResponseFilteredIterator;
import it.unimi.dsi.law.warc.io.WarcRecord;
import it.unimi.dsi.law.warc.parser.HTMLParser;
import it.unimi.dsi.law.warc.parser.HTMLParser.SetLinkReceiver;
import it.unimi.dsi.law.warc.util.HttpResponse;
import it.unimi.dsi.law.warc.util.WarcHttpResponse;
import it.unimi.dsi.logging.ProgressLogger;

// RELEASE-STATUS: DIST

/** Extracts links from a WARC file.
 *
 * <p>This class scans a WARC file, parsing pages and extracting links. Links are resolved using
 * a given {@link StringMap}. The resulting successors lists are given one
 * per line, with the index of the source node followed by the outdegree, followed by successor indices.
 *
 * <p>Optionally, it is possible to specify a secondary {@link StringMap} for duplicates. It
 * must return, for each duplicate, the number of the corresponding archetype (the page that it is equal to). This
 * map is usually a {@link it.unimi.dsi.law.warc.util.RemappedStringMap}.
 */

public class ExtractLinks {
	private final static Logger LOGGER = LoggerFactory.getLogger(ExtractLinks.class);

	final public static String DEFAULT_BUFFER_SIZE = "64Ki";

	/** Extracts links from a WARC file.
	 *
	 * @param in the WARC file as an input stream.
	 * @param isGZipped whether <code>in</code> is compressed.
	 * @param filter the filter.
	 * @param pw a print writer there the links will be printed in ASCII format (node number followed by successors).
	 * @param urls the term map for URLs.
	 * @param duplicates the term map for duplicate URLs.
	 */

	public static void run(final FastBufferedInputStream in, final boolean isGZipped, final Filter<HttpResponse> filter, final PrintWriter pw, final StringMap<? extends CharSequence> urls, final StringMap<? extends CharSequence> duplicates) throws IOException {
		final WarcRecord record = isGZipped ? new GZWarcRecord() : new WarcRecord();
		final WarcHttpResponse response = new WarcHttpResponse();
		final HttpResponseFilteredIterator it = new HttpResponseFilteredIterator(in, record, response, filter);
		// TODO: check this size
		final HTMLParser parser = new HTMLParser();
		final SetLinkReceiver setLinkReceiver = new SetLinkReceiver();
		final IntOpenHashSet successors = new IntOpenHashSet();
		int[] successor = IntArrays.EMPTY_ARRAY;

		final ProgressLogger pl = new ProgressLogger(LOGGER, 1, TimeUnit.MINUTES, "pages");

		pl.start("Extracting...");
		int k, d;

		while (it.hasNext()) {
			it.next();

			if (urls != null) {
				k = (int)urls.getLong(response.uri().toString());
				if (response.isDuplicate()) {
					if (k >= 0) {
						LOGGER.error("URL " + response.uri() + " is contained in the URL map but it is a duplicate");
						pw.println(k);
						pl.update();
					}
					continue;
				}

				if (k == -1) {
					LOGGER.error("URL " + response.uri() + " is not contained in the URL map; this may happen if the original digest/URL file was sorted unstably or if there are several non-duplicate pages with the same digest");
					continue;
				}
				pw.print(k);
				pw.print('\t');
				parser.parse(response, setLinkReceiver);
				successors.clear();

				for (URI url : setLinkReceiver) {
					if ((k = (int)urls.getLong(url.toString())) != -1) {
						LOGGER.debug("Adding successor " + url + ":" + k);
						successors.add(k);
					}
					else if (duplicates != null && (k = (int)duplicates.getLong(url.toString())) != -1) {
						LOGGER.debug("Adding duplicate " + url + ":" + k);
						successors.add(k);
					}
				}

				d = successors.size(); // Outdegree
				successors.toArray(successor = IntArrays.grow(successor, d, 0));
				IntArrays.quickSort(successor, 0, d);

				for(int i = 0; i < d; i++) {
					pw.print(successor[i]);
					pw.print('\t');
				}
			}
			else {
				pw.print(response.uri());
				parser.parse(response, setLinkReceiver);
				for (URI url : setLinkReceiver) {
					pw.print('\t');
					pw.print(url);
				}
			}

			pw.println();
			pl.update();
		}
		pl.done();
	}
	@SuppressWarnings("unchecked")
	public static void main(String arg[]) throws Exception {
		SimpleJSAP jsap = new SimpleJSAP(ExtractLinks.class.getName(), "Extract links in pages from a WARC file.",
				new Parameter[] {
			new FlaggedOption("bufferSize", JSAP.INTSIZE_PARSER, DEFAULT_BUFFER_SIZE, JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer."),
			new Switch("gzip", 'z', "gzip", "Tells if the warc is compressed."),
			new FlaggedOption("filter", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "filter", "The filter."),
			new FlaggedOption("start", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's', "start", "The starting offset (in bytes) in the WARC file (mainly for debugging purposes)."),
			new FlaggedOption("duplicates", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "duplicates", "The (remapped) term map for duplicate URLs. If not present, only links pointing to URLs in <urls> will be used."),
			new FlaggedOption("urls", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'u', "The term map for the node URLs."),
			new UnflaggedOption("warcFile", JSAP.STRING_PARSER, "-", JSAP.REQUIRED, JSAP.NOT_GREEDY, "The WARC file basename (if not present, or -, stdin will be used)."),
		});

		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final boolean isGZipped = jsapResult.getBoolean("gzip");
		final String filterSting = jsapResult.getString("filter") == null ? "TRUE" : jsapResult.getString("filter");
		final String warcFile = jsapResult.getString("warcFile");
		final int bufferSize = jsapResult.getInt("bufferSize");

		final Filter<HttpResponse> filter = new FilterParser<HttpResponse>(HttpResponse.class).parse(filterSting);

		final StringMap<? extends CharSequence> urls = (StringMap<? extends CharSequence>)(jsapResult.userSpecified("urls") ? BinIO.loadObject(jsapResult.getString("urls")) : null);
		final StringMap<? extends CharSequence> duplicates = (StringMap<? extends CharSequence>)(jsapResult.userSpecified("duplicates") ? BinIO.loadObject(jsapResult.getString("duplicates")) : null);

		final FastBufferedInputStream in = new FastBufferedInputStream(warcFile.equals("-") ? System.in : new FileInputStream(new File(warcFile + ".warc" + (isGZipped ? ".gz" : ""))), bufferSize);
		if (jsapResult.userSpecified("start")) in.skip(jsapResult.getLong("start"));
		final PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FastBufferedOutputStream(System.out, bufferSize), "ASCII"));

		try {
			run(in, isGZipped, filter, pw, urls, duplicates);
		}
		finally {
			in.close();
			pw.close();
		}
	}
}

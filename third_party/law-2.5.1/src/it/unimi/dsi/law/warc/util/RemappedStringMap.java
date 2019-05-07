package it.unimi.dsi.law.warc.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;

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

import it.unimi.dsi.big.io.FileLinesCollection;
import it.unimi.dsi.big.util.ShiftAddXorSignedStringMap;
import it.unimi.dsi.big.util.StringMap;
import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.AbstractObject2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectBigList;
import it.unimi.dsi.sux4j.mph.MWHCFunction;

// RELEASE-STATUS: DIST

/** A {@link StringMap} that remaps values returned by another {@link StringMap}.
 *
 * <p>
 * Instances of this class wrap a given minimal perfect hash
 * and a given map (an integer array). Queries to {@link #getLong(Object)} are
 * solved by first inquiring the given map.
 * If the result is -1, it is returned; otherwise, we use the result to index
 * the map and return the corresponding element.
 */

public class RemappedStringMap extends AbstractObject2LongFunction<CharSequence> implements StringMap<CharSequence>, Serializable {

	final public static String DEFAULT_BUFFER_SIZE = "64Ki";

	private static final long serialVersionUID = 1L;
	/** The underlying string map. */
	private final StringMap<? extends CharSequence> stringMap;
	/** The remapping array. */
	private final int[] map;

	/** Creates a new remapped minimal perfect hash.
	 *
	 * @param stringMap the underlying minimal perfect hash.
	 * @param map a map that will be used to remap the numbers returned by <code>mph</code>.
	 */

	public RemappedStringMap(final StringMap<? extends CharSequence> stringMap, final int[] map) {
		if (stringMap.size64() != map.length) throw new IllegalArgumentException("Minimal perfect hash size (" + stringMap.size64() + ") is not equal to map length (" + map.length + ")");
		this.stringMap = stringMap;
		this.map = map;
	}

	public long getLong(Object o) {
		CharSequence term = (CharSequence)o;
		final int x = (int)stringMap.getLong(term);
		if (x == -1) return -1;
		return map[x];
	}

	public long size64() {
		return stringMap.size64();
	}

	public int size() {
		return (int)Math.min(Integer.MAX_VALUE, size64());
	}

	public static void run(String duplicateURLs, String archetypeURLs, StringMap<? extends CharSequence> resolver, String remappedFilename, int bufferSize) throws IOException {

		@SuppressWarnings("resource")
		final FastBufferedInputStream arch = new FastBufferedInputStream(new FileInputStream(archetypeURLs), bufferSize);

		// First we build a signed minimal perfect hash for duplicates.
		FileLinesCollection flc = new FileLinesCollection(duplicateURLs, "ASCII");
		final StringMap<CharSequence> duplicateMph = new ShiftAddXorSignedStringMap(flc.iterator(), new MWHCFunction.Builder<CharSequence>().keys(flc).transform(TransformationStrategies.utf16()).build());

		// TODO: works only for less than Integer.MAX_VALUE duplicates.
		if (duplicateMph.size64() > Integer.MAX_VALUE) throw new IndexOutOfBoundsException();

		byte[] line = new byte[2048];
		int[] map = new int[(int)duplicateMph.size64()];
		int start, len;
		ByteArrayCharSequence s = new ByteArrayCharSequence();

		for(int n = 0; ; n++) {
			start = 0;
			while ((len = arch.readLine(line, start, line.length - start)) == line.length - start) {
				start += len;
				line = ByteArrays.grow(line, line.length + 1);
			};

			len = start + Math.max(len, 0);
			if (len == 0) break;
			// TODO: this is really inefficient (used to peek directly into the
			map[n] = (int)resolver.getLong(s.wrap(line, 0, len));
			if (map[n] == - 1) throw new IllegalArgumentException("URL " + new String(line, 0, len, "ASCII") + " cannot be resolved");
		}

		BinIO.storeObject(new RemappedStringMap(duplicateMph, map), remappedFilename);

		arch.close();
	}
	public ObjectBigList<CharSequence> list() {
		return null;
	}

	public boolean containsKey(final Object o) {
		return stringMap.containsKey(o);
	}

	@SuppressWarnings("unchecked")
	public static void main(String arg[]) throws Exception {
		SimpleJSAP jsap = new SimpleJSAP(RemappedStringMap.class.getName(),
				"Builds a remapped minimal perfect hash by reading two parallel files (duplicates and archetypes), and mapping each line of the first file to the number returned by a given minimal perfect hash on the corresponding line of the second file.",
				new Parameter[] {
			new FlaggedOption("bufferSize", JSAP.INTSIZE_PARSER, DEFAULT_BUFFER_SIZE, JSAP.NOT_REQUIRED, 'b', "buffer-size", "The size of an I/O buffer."),
			new UnflaggedOption("duplicateURLs", JSAP.STRING_PARSER, JSAP.REQUIRED, "The duplicate file."),
			new UnflaggedOption("archetypeURLs", JSAP.STRING_PARSER, JSAP.REQUIRED, "The archetype file."),
			new UnflaggedOption("resolver", JSAP.STRING_PARSER, JSAP.REQUIRED, "The term map used to resolve the second field."),
			new UnflaggedOption("remappedMph", JSAP.STRING_PARSER, JSAP.REQUIRED, "The resulting remapped minimal perfect hash."),
		});
		JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		run(jsapResult.getString("duplicateURLs"), jsapResult.getString("archetypeURLs"), (StringMap<? extends CharSequence>)BinIO.loadObject(jsapResult.getString("resolver")), jsapResult.getString("remappedMph"), jsapResult.getInt("bufferSize"));
	}
}

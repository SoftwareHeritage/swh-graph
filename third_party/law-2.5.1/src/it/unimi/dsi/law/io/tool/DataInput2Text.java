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

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Reference2IntOpenHashMap;

// RELEASE-STATUS: DIST

/**
 * The main method of this class converts a binary {@link DataOutput} file containing numbers to text format.
 */
public class DataInput2Text {

	private DataInput2Text() {}

	protected static enum Type { BYTE, SHORT, INT, LONG, FLOAT, DOUBLE };
	private static final Reference2IntOpenHashMap<Type> type2size = new Reference2IntOpenHashMap<>(
			new Type[] { Type.BYTE, Type.SHORT, Type.INT, Type.LONG, Type.FLOAT, Type.DOUBLE }, new int[] {  1, 2, 4, 8, 4, 8 }
	);

	/** Skips the given number of items; returns true if we went beyond EOF. */
	private static boolean skipItems(final FastBufferedInputStream fbis, final long l, final int sizeOfItem) throws IOException {
		if (l == 0) return false;
		final long toBeSkipped = sizeOfItem * l;
		return fbis.skip(toBeSkipped) < toBeSkipped;
	}

	public static void main(final String[] arg) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(Text2DataOutput.class.getName(), "Converts a binary (DataOutput) file containing numbers to text format.",
				new Parameter[] {
					new UnflaggedOption("binaryFile", JSAP.STRING_PARSER, "-", JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The input binary file, - for stdin."),
					new UnflaggedOption("textFile", JSAP.STRING_PARSER, "-", JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The output text file, - for stdout."),
					new FlaggedOption("type", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "type", "The binary data type (byte, int, short, long, double, float)."),
					new FlaggedOption("from", JSAP.LONGSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'f', "from", "Start from the given item (inclusive), 0-based."),
					new FlaggedOption("number", JSAP.LONGSIZE_PARSER, Long.toString(Long.MAX_VALUE), JSAP.NOT_REQUIRED, 'n', "number", "Maximum number of elements that will be output."),
					new FlaggedOption("step", JSAP.LONGSIZE_PARSER, "1", JSAP.NOT_REQUIRED, 's', "step", "Only output one out of step elements."),
			});

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final Type type = Type.valueOf(jsapResult.getString("type").toUpperCase());
		final int sizeOfItem = type2size.getInt(type);
		final long fromItem = jsapResult.getLong("from");
		final long step = jsapResult.getLong("step");
		long howManyItems = jsapResult.getLong("number");

		final String inFilename = jsapResult.getString("binaryFile");
		final String outFilename = jsapResult.getString("textFile");

		// This double-buffers stdin, but gives us a uniform interface to skipping.
		final FastBufferedInputStream fbis = new FastBufferedInputStream(inFilename.equals("-") ? System.in : new FileInputStream(inFilename));
		final DataInputStream dis = new DataInputStream(fbis);
		final PrintStream out = outFilename.equals("-") ? System.out : new PrintStream(new FastBufferedOutputStream(new FileOutputStream(outFilename)), false, Charsets.ISO_8859_1.toString());

		skipItems(fbis, fromItem, sizeOfItem);

		try {
			switch (type) {
			case BYTE:
				while (howManyItems-- > 0) {
					out.println(dis.readByte());
					if (step > 1 && skipItems(fbis, step - 1, sizeOfItem)) break;
				}
				break;
			case SHORT:
				while (howManyItems-- > 0) {
					out.println(dis.readShort());
					if (step > 1 && skipItems(fbis, step - 1, sizeOfItem)) break;
				}
				break;
			case INT:
				while (howManyItems-- > 0) {
					out.println(dis.readInt());
					if (step > 1 && skipItems(fbis, step - 1, sizeOfItem)) break;
				}
				break;
			case LONG:
				while (howManyItems-- > 0) {
					out.println(dis.readLong());
					if (step > 1 && skipItems(fbis, step - 1, sizeOfItem)) break;
				}
				break;
			case FLOAT:
				while (howManyItems-- > 0) {
					out.println(dis.readFloat());
					if (step > 1 && skipItems(fbis, step - 1, sizeOfItem)) break;
				}
				break;
			case DOUBLE:
				while (howManyItems-- > 0) {
					out.println(dis.readDouble());
					if (step > 1 && skipItems(fbis, step - 1, sizeOfItem)) break;
				}
				break;
			}
		}
		catch (final EOFException eof) {}

		dis.close();
		out.close();
	}
}

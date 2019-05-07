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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.law.io.tool.DataInput2Text.Type;

// RELEASE-STATUS: DIST

/** The main method of this class converts converts a text file containing numbers to binary {@link DataOutput} format. The input must be in a format understandable by
 * the {@code decode()} method of integer primivite types (e.g., {@link Integer#decode(String)}). */

public class Text2DataOutput {

	private Text2DataOutput() {}

	public static void main(final String[] arg) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(Text2DataOutput.class.getName(), "Converts a text file containing numbers to binary (DataOutput) format. Inputs of integer type can be written in any format accepted by the decode() family of parsing methods.",
				new Parameter[] {
					new FlaggedOption("type", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 't', "type", "The binary data type (byte, int, short, long, double, float)."),
					new UnflaggedOption("textFile", JSAP.STRING_PARSER, "-", JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The intput text file, - for stdin."),
					new UnflaggedOption("binaryFile", JSAP.STRING_PARSER, "-", JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The output binary file, - for stdout."),
			});

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final Type type = Type.valueOf(jsapResult.getString("type").toUpperCase());
		final String inFilename = jsapResult.getString("textFile");
		final String outFilename = jsapResult.getString("binaryFile");
		final FastBufferedReader in = new FastBufferedReader(new InputStreamReader(inFilename.equals("-") ? System.in : new FileInputStream(inFilename), Charsets.ISO_8859_1.toString()));
		final DataOutputStream out = new DataOutputStream(new FastBufferedOutputStream(outFilename.equals("-") ? System.out : new FileOutputStream(outFilename)));

		final MutableString s = new MutableString();

		switch (type) {
			case BYTE:
				while ((in.readLine(s)) != null) out.writeByte(Byte.decode(s.trimRight().toString()).byteValue());
				break;
			case INT:
				while ((in.readLine(s)) != null) out.writeInt(Integer.decode(s.trimRight().toString()).intValue());
				break;
			case SHORT:
				while ((in.readLine(s)) != null) out.writeShort(Short.decode(s.trimRight().toString()).shortValue());
				break;
			case LONG:
				while ((in.readLine(s)) != null) out.writeLong(Long.decode(s.trimRight().toString()).longValue());
				break;
			case FLOAT:
				while ((in.readLine(s)) != null) out.writeFloat(Float.parseFloat(s.trimRight().toString()));
				break;
			case DOUBLE:
				while ((in.readLine(s)) != null) out.writeDouble(Double.parseDouble(s.trimRight().toString()));
				break;
		}

		in.close();
		out.close();
	}
}

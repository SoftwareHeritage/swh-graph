package it.unimi.dsi.law.stat;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.martiansoftware.jsap.JSAPResult;

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

import it.unimi.dsi.fastutil.doubles.DoubleIterators;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.law.util.Precision;

// RELEASE-STATUS: DIST

/** An abstract class providing basic infrastructure for all classes computing some correlation index between two score vectors,
 * such as {@link KendallTau}, {@link WeightedTau} and {@link AveragePrecisionCorrelation}.
 *
 * <p>Implementing classes have just to implement {@link #compute(double[], double[])} to get a wealth of support method,
 * including {@linkplain #loadAsDoubles(CharSequence, Class, boolean) loading data in different formats}
 * and {@linkplain #parseInputTypes(JSAPResult) parsing file types}.
 */

public abstract class CorrelationIndex {

	protected CorrelationIndex() {}

	/** Computes the correlation between two score vectors.
	 *
	 * <p>Note that this method must be called with some care if you're right on memory. More precisely, the two
	 * arguments should be built on the fly in the method call, and not stored in variables,
	 * as the some of the argument arrays might be {@code null}'d during the execution of this method
	 * to free some memory: if the array is referenced elsewhere the garbage collector will not
	 * be able to collect it.
	 *
	 * @param v0 the first score vector.
	 * @param v1 the second score vector; in asymmetric correlation indices, this should be the reference score.
	 * @return the correlation.
	 */
	public abstract double compute(double v0[], double v1[]);

	/** Computes the correlation between two score vectors.
	 *
	 * @param f0 the binary file of doubles containing the first score vector.
	 * @param f1 the binary file of doubles containing the second score vector.
	 * @return the correlation.
	 */
	public double computeDoubles(final CharSequence f0, final CharSequence f1) throws IOException {
		return computeDoubles(f0, f1, Integer.MAX_VALUE);
	}

	/** Computes the correlation between two (possible reversed) score vectors.
	 *
	 * @param f0 the binary file of doubles containing the first score vector.
	 * @param f1 the binary file of doubles containing the second score vector.
	 * @param reverse whether to reverse the ranking induced by the score vectors by loading opposite values.
	 * @return the correlation.
	 */
	public double computeDoubles(final CharSequence f0, final CharSequence f1, final boolean reverse) throws IOException {
		return computeDoubles(f0, f1, reverse, Integer.MAX_VALUE);
	}

	/** Computes the correlation between two score vectors with a given precision.
	 *
	 * @param f0 the binary file of doubles containing the first score vector.
	 * @param f1 the binary file of doubles containing the second score vector.
	 * @param digits the number of digits to be preserved when computing the correlation.
	 * @return the correlation.
	 * @see Precision#truncate(double[], int)
	 */
	public double computeDoubles(final CharSequence f0, final CharSequence f1, final int digits) throws IOException {
		return computeDoubles(f0, f1, false, digits);
	}

	/** Computes the correlation between two (possible reversed) score vectors with a given precision.
	 *
	 * @param f0 the binary file of doubles containing the first score vector.
	 * @param f1 the binary file of doubles containing the second score vector.
	 * @param reverse whether to reverse the ranking induced by the score vectors by loading opposite values.
	 * @param digits the number of digits to be preserved when computing the correlation.
	 * @return the correlation.
	 * @see Precision#truncate(double[], int)
	 */
	public double computeDoubles(final CharSequence f0, final CharSequence f1, final boolean reverse, final int digits) throws IOException {
		return compute(f0, Double.class, f1, Double.class, reverse, digits);
	}

	/** Computes the correlation between two score vectors.
	 *
	 * @param f0 the binary file of floats containing the first score vector.
	 * @param f1 the binary file of floats containing the second score vector.
	 * @return the correlation.
	 */
	public double computeFloats(final CharSequence f0, final CharSequence f1) throws IOException {
		return computeFloats(f0, f1, Integer.MAX_VALUE);
	}

	/** Computes the correlation between two (possibly reversed) score vectors.
	 *
	 * @param f0 the binary file of floats containing the first score vector.
	 * @param f1 the binary file of floats containing the second score vector.
	 * @param reverse whether to reverse the ranking induced by the score vectors by loading opposite values.
	 * @return the correlation.
	 */
	public double computeFloats(final CharSequence f0, final CharSequence f1, final boolean reverse) throws IOException {
		return computeFloats(f0, f1, reverse, Integer.MAX_VALUE);
	}

	/** Computes the correlation between two score vectors with a given precision.
	 *
	 * @param f0 the binary file of floats containing the first score vector.
	 * @param f1 the binary file of floats containing the second score vector.
	 * @param digits the number of digits to be preserved when computing the correlation.
	 * @return the correlation.
	 * @see Precision#truncate(double[], int)
	 */
	public double computeFloats(final CharSequence f0, final CharSequence f1, final int digits) throws IOException {
		return computeFloats(f0, f1, false, digits);
	}

	/** Computes the correlation between two (possibly reversed) score vectors with a given precision.
	 *
	 * @param f0 the binary file of floats containing the first score vector.
	 * @param f1 the binary file of floats containing the second score vector.
	 * @param digits the number of digits to be preserved when computing the correlation.
	 * @param reverse whether to reverse the ranking induced by the score vectors by loading opposite values.
	 * @return the correlation.
	 * @see Precision#truncate(double[], int)
	 */
	public double computeFloats(final CharSequence f0, final CharSequence f1, final boolean reverse, final int digits) throws IOException {
		return compute(f0, Float.class, f1, Float.class, reverse, digits);
	}

	/** Computes the correlation between two score vectors.
	 *
	 * @param f0 the binary file of integers containing the first score vector.
	 * @param f1 the binary file of integers containing the second score vector.
	 * @return the correlation.
	 */
	public double computeInts(final CharSequence f0, final CharSequence f1) throws IOException {
		return computeInts(f0, f1, false);
	}

	/** Computes the correlation between two (possibly reversed) score vectors.
	 *
	 * @param f0 the binary file of integers containing the first score vector.
	 * @param f1 the binary file of integers containing the second score vector.
	 * @param reverse whether to reverse the ranking induced by the score vectors by loading opposite values.
	 * @return the correlation.
	 */
	public double computeInts(final CharSequence f0, final CharSequence f1, final boolean reverse) throws IOException {
		return compute(f0, Integer.class, f1, Integer.class, reverse, Integer.MAX_VALUE);
	}

	/** Computes the correlation between two score vectors.
	 *
	 * @param f0 the binary file of longs containing the first score vector.
	 * @param f1 the binary file of longs containing the second score vector.
	 * @return the correlation.
	 */
	public double computeLongs(final CharSequence f0, final CharSequence f1) throws IOException {
		return computeLongs(f0, f1, false);
	}

	/** Computes the correlation between (possibly reversed) two score vectors.
	 *
	 * @param f0 the binary file of longs containing the first score vector.
	 * @param f1 the binary file of longs containing the second score vector.
	 * @param reverse whether to reverse the ranking induced by the score vectors by loading opposite values.
	 * @return the correlation.
	 */
	public double computeLongs(final CharSequence f0, final CharSequence f1, final boolean reverse) throws IOException {
		return compute(f0, Long.class, f1, Long.class, reverse, Integer.MAX_VALUE);
	}

	/** Computes the correlation between two (possibly reversed) score vectors with a given precision.
	 *
	 * @param f0 the file containing the first score vector.
	 * @param inputType0 the input type of the first score vector.
	 * @param f1 the file containing the second score vector.
	 * @param inputType1 the input type of the second score vector.
	 * @param reverse whether to reverse the ranking induced by the score vectors by loading opposite values.
	 * @param digits the number of digits to be preserved when computing the correlation.
	 * they are assumed to be in binary format.
	 * @return the correlation.
	 * @see Precision#truncate(double[], int)
	 */
	public double compute(final CharSequence f0, final Class<?> inputType0, final CharSequence f1, final Class<?> inputType1, final boolean reverse, final int digits) throws IOException {
		return compute(Precision.truncate(loadAsDoubles(f0, inputType0, reverse), digits), Precision.truncate(loadAsDoubles(f1, inputType1, reverse), digits));
	}

	/** Computes the correlation between two (possibly reversed) score vectors.
	 *
	 * @param f0 the file containing the first score vector.
	 * @param inputType0 the input type of the first score vector.
	 * @param f1 the file containing the second score vector.
	 * @param inputType1 the input type of the second score vector.
	 * @param reverse whether to reverse the ranking induced by the score vectors by loading opposite values.
	 * they are assumed to be in binary format.
	 * @return the correlation.
	 * @see Precision#truncate(double[], int)
	 */
	public double compute(final CharSequence f0, final Class<?> inputType0, final CharSequence f1, final Class<?> inputType1, final boolean reverse) throws IOException {
		return compute(Precision.truncate(loadAsDoubles(f0, inputType0, reverse), Integer.MAX_VALUE), Precision.truncate(loadAsDoubles(f1, inputType1, reverse), Integer.MAX_VALUE));
	}

	/** Computes the correlation between two score vectors with a given precision.
	 *
	 * @param f0 the file containing the first score vector.
	 * @param inputType0 the input type of the first score vector.
	 * @param f1 the file containing the second score vector.
	 * @param inputType1 the input type of the second score vector.
	 * @param digits the number of digits to be preserved when computing the correlation.
	 * they are assumed to be in binary format.
	 * @return the correlation.
	 * @see Precision#truncate(double[], int)
	 */
	public double compute(final CharSequence f0, final Class<?> inputType0, final CharSequence f1, final Class<?> inputType1, final int digits) throws IOException {
		return compute(Precision.truncate(loadAsDoubles(f0, inputType0, false), digits), Precision.truncate(loadAsDoubles(f1, inputType1, false), digits));
	}

	/** Computes the correlation between two score vectors.
	 *
	 * @param f0 the file containing the first score vector.
	 * @param f1 the file containing the second score vector.
	 * @param inputType the input type.
	 * @return the correlation.
	 * @see Precision#truncate(double[], int)
	 */
	public double compute(final CharSequence f0, final CharSequence f1, final Class<?> inputType) throws IOException {
		return compute(f0, inputType, f1, inputType, Integer.MAX_VALUE);
	}

	/** Loads a vector of doubles, either in binary or textual form.
	 *
	 * @param f a filename.
	 * @param inputType the input type, expressed as a class: {@link Double}, {@link Float}, {@link Integer}, {@link Long}
	 * or {@link String} to denote a text file.
	 * @param reverse whether to reverse the ranking induced by the score vector by loading opposite values.
	 * @return an array of double obtained reading <code>f</code>.
	 * @throws IllegalArgumentException if {@code reverse} is true, the type is integer or long and
	 * {@link Integer#MIN_VALUE} or {@link Long#MIN_VALUE}, respectively, appear in the file, as we
	 * cannot take the opposite.
	 */
	public static double[] loadAsDoubles(final CharSequence f, final Class<?> inputType, final boolean reverse) throws IOException {
		final double[] array;
		if (inputType == String.class) {
			array = DoubleIterators.unwrap(TextIO.asDoubleIterator(f));
			if (reverse) for(int i = array.length; i-- != 0;) array[i] = -array[i];
			return array;
		}
		final File file = new File(f.toString());
		long length;
		final FileInputStream fis = new FileInputStream(file);
		final DataInputStream dis = new DataInputStream(new FastBufferedInputStream(fis));
		try {
			if (inputType == Integer.class || inputType == Float.class) length = fis.getChannel().size() / 4;
			else if (inputType == Long.class || inputType == Double.class) length = fis.getChannel().size() / 8;
			else throw new IllegalArgumentException();
			if (length > Integer.MAX_VALUE) throw new IllegalArgumentException("File too long: " + fis.getChannel().size()+ " bytes (" + length + " elements)");
			array = new double[(int)length];

			if (reverse) {
			if (inputType == Float.class) for(int i = 0; i < length; i++) array[i] = -dis.readFloat();
			if (inputType == Double.class) for(int i = 0; i < length; i++) array[i] = -dis.readDouble();
			if (inputType == Integer.class)
				for(int i = 0; i < length; i++) {
					array[i] = -dis.readInt();
					if (array[i] == Integer.MIN_VALUE) throw new IllegalArgumentException("The score vector " + f + " contains Integer.MIN_VALUE, whose opposite cannot be represented");
				}
			if (inputType == Long.class)
				for(int i = 0; i < length; i++) {
					array[i] = -dis.readLong();
					if (array[i] == Long.MIN_VALUE) throw new IllegalArgumentException("The score vector " + f + " contains Long.MIN_VALUE, whose opposite cannot be represented");
				}
			}
			else {
				if (inputType == Float.class) for(int i = 0; i < length; i++) array[i] = dis.readFloat();
				if (inputType == Double.class) for(int i = 0; i < length; i++) array[i] = dis.readDouble();
				if (inputType == Integer.class) for(int i = 0; i < length; i++) array[i] = dis.readInt();
				if (inputType == Long.class) for(int i = 0; i < length; i++) array[i] = dis.readLong();
			}
		}
		finally {
			dis.close();
		}
		return array;
	}

	private static final Class<?>[] DOUBLES_DOUBLES = new Class<?>[] { Double.class, Double.class };

	/** Commodity method to extract from a {@link JSAPResult} instance the file type information provided by
	 * the user, or supply the default (doubles in binary form). We look into the parameter {@code type}
	 * and we look for either a single type, or two types separated by a colon. The types can be
	 * {@code double}, {@code float}, {@code int}, {@code long} or {@code text}. If the parameter is not
	 * specified, we return the type {@link Double} for both formats.
	 *
	 * @param jsapResult the result of the parsing of a command line.
	 * @return a array containing two classes, representing the type of the files to be loaded (using {@link #loadAsDoubles(CharSequence, Class, boolean)}'s
	 * conventions).
	 */
	public static Class<?>[] parseInputTypes(final JSAPResult jsapResult) {
		if (jsapResult.userSpecified("type")) {
			Class<?>[] inputType =  new Class<?>[2];
			String[] type = new String[2];
			String types = jsapResult.getString("type");
			int pos = types.indexOf(':');
			if (pos >= 0) {
				type[0] = types.substring(0, pos);
				type[1] = types.substring(pos + 1);
			}
			else type[0] = type[1] = types;
			for (int i = 0; i < 2; i++)
				if (type[i].equals("int")) inputType[i] = Integer.class;
				else if (type[i].equals("long")) inputType[i] = Long.class;
				else if (type[i].equals("float")) inputType[i] = Float.class;
				else if (type[i].equals("double")) inputType[i] = Double.class;
				else if (type[i].equals("text")) inputType[i] = String.class;
				else throw new IllegalArgumentException("Type \"" + type[i] + "\" is not one of int, long, float, double, text");

			return inputType;
		}
		else return DOUBLES_DOUBLES;
	}
}

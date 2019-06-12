package it.unimi.dsi.big.webgraph.labelling;

/*
 * Copyright (C) 2007-2017 Paolo Boldi and Sebastiano Vigna
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

/** An abstract (single-attribute) integer label.
 *
 * <p>This class provides basic methods for a label holding an integer.
 * Concrete implementations may impose further requirements on the integer.
 *
 * <p>Implementing subclasses must provide constructors, {@link Label#copy()},
 * {@link Label#fromBitStream(it.unimi.dsi.io.InputBitStream, long)}, {@link Label#toBitStream(it.unimi.dsi.io.OutputBitStream, long)}
 * and possibly override {@link #toString()}.
 */

public abstract class AbstractIntLabel extends AbstractLabel implements Label {
	/** The key of the attribute represented by this label. */
	protected final String key;
	/** The value of the attribute represented by this label. */
	public int value;

	/** Creates an int label with given key and value.
	 *
	 * @param key the (only) key of this label.
	 * @param value the value of this label.
	 */
	public AbstractIntLabel(String key, int value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public String wellKnownAttributeKey() {
		return key;
	}

	@Override
	public String[] attributeKeys() {
		return new String[] { key };
	}

	@Override
	public Class<?>[] attributeTypes() {
		return new Class[] { int.class };
	}

	@Override
	public Object get(String key) {
		return Integer.valueOf(getInt(key));
	}

	@Override
	public int getInt(String key) {
		if (this.key.equals(key)) return value;
		throw new IllegalArgumentException("Unknown key " + key);
	}

	@Override
	public long getLong(String key) {
		return getInt(key);
	}

	@Override
	public float getFloat(String key) {
		return getInt(key);
	}

	@Override
	public double getDouble(String key) {
		return getInt(key);
	}

	@Override
	public Object get() {
		return Integer.valueOf(getInt());
	}

	@Override
	public int getInt() {
		return value;
	}

	@Override
	public long getLong() {
		return value;
	}

	@Override
	public float getFloat() {
		return value;
	}

	@Override
	public double getDouble() {
		return value;
	}

	@Override
	public String toString() {
		return key + ":" + value;
	}

	@Override
	public boolean equals(Object x) {
		if (x instanceof AbstractIntLabel) return (value == ((AbstractIntLabel)x).value);
		else return false;
	}

	@Override
	public int hashCode() {
		return value;
	}
}
// TODO: should be in webgraph upstream
// package it.unimi.dsi.big.webgraph.labelling;
package org.softwareheritage.graph.labels;

/*
 * Copyright (C) 2020 TODO
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

import it.unimi.dsi.big.webgraph.labelling.Label;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.util.Arrays;

/**
 * A list of longs represented in fixed width. Each list is prefixed by its length written in
 * {@linkplain OutputBitStream#writeGamma(int) &gamma; coding}.
 */

public class FixedWidthLongListLabel extends org.softwareheritage.graph.labels.AbstractLongListLabel {
    /** The bit width used to represent the value of this label. */
    private final int width;

    /**
     * Creates a new fixed-width long label.
     *
     * @param key the (only) key of this label.
     * @param width the label width (in bits).
     * @param value the value of this label.
     */
    public FixedWidthLongListLabel(String key, int width, long[] value) {
        super(key, value);
        for (int i = value.length; i-- != 0;)
            if (value[i] < 0 || value[i] >= 1L << width)
                throw new IllegalArgumentException("Value out of range: " + Long.toString(value[i]));
        this.width = width;
    }

    /**
     * Creates a new fixed-width label with an empty list.
     *
     * @param key the (only) key of this label.
     * @param width the label width (in bits).
     */
    public FixedWidthLongListLabel(String key, int width) {
        this(key, width, LongArrays.EMPTY_ARRAY);
    }

    /**
     * Creates a new fixed-width long label using the given key and width with an empty list.
     *
     * @param arg two strings containing the key and the width of this label.
     */
    public FixedWidthLongListLabel(String... arg) {
        this(arg[0], Integer.parseInt(arg[1]));
    }

    @Override
    public Label copy() {
        return new FixedWidthLongListLabel(key, width, value.clone());
    }

    @Override
    public int fromBitStream(InputBitStream inputBitStream, final long sourceUnused) throws IOException {
        long readBits = inputBitStream.readBits();
        value = new long[inputBitStream.readGamma()];
        for (int i = 0; i < value.length; i++)
            value[i] = inputBitStream.readLong(width);
        return (int) (inputBitStream.readBits() - readBits);
    }

    @Override
    public int toBitStream(OutputBitStream outputBitStream, final long sourceUnused) throws IOException {
        int bits = outputBitStream.writeGamma(value.length);
        for (int i = 0; i < value.length; i++)
            bits += outputBitStream.writeLong(value[i], width);
        return bits;
    }

    /**
     * Returns -1 (the fixed width refers to a single long, not to the entire list).
     *
     * @return -1;
     */
    @Override
    public int fixedWidth() {
        return -1;
    }

    @Override
    public String toString() {
        return key + ":" + Arrays.toString(value) + " (width:" + width + ")";
    }

    @Override
    public String toSpec() {
        return this.getClass().getName() + "(" + key + "," + width + ")";
    }
}

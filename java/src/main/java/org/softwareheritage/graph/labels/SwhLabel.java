/*
 * Copyright (c) 2021-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.labels;

import it.unimi.dsi.big.webgraph.labelling.AbstractLabel;
import it.unimi.dsi.big.webgraph.labelling.FixedWidthLongListLabel;
import it.unimi.dsi.big.webgraph.labelling.Label;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;
import java.util.Arrays;

/**
 * Software Heritage graph edge labels following Webgraph labels convention.
 *
 * @author The Software Heritage developers
 */

public class SwhLabel extends AbstractLabel {
    private final String key;
    private final int width;
    // TODO: in the future we would like this to be edge type dependent (eg: having a similar SnpEntry
    // to store branch names)
    public DirEntry[] value;
    // Use existing Webgraph class to represent a list of DirEntry as a list of encoded long
    private final FixedWidthLongListLabel longList;

    private static final DirEntry[] EMPTY_ARRAY = {};

    public SwhLabel(String key, int width, DirEntry[] value) {
        this.key = key;
        this.width = width;
        this.value = value;

        long[] valueEncoded = new long[value.length];
        for (int i = 0; i < value.length; i++)
            valueEncoded[i] = value[i].toEncoded();
        this.longList = new FixedWidthLongListLabel(key, width, valueEncoded);
    }

    public SwhLabel(String key, int width) {
        this(key, width, EMPTY_ARRAY);
    }

    public SwhLabel(String... arg) {
        this(arg[0], Integer.parseInt(arg[1]));
    }

    @Override
    public int fromBitStream(InputBitStream inputBitStream, final long sourceUnused) throws IOException {
        int ret = longList.fromBitStream(inputBitStream, sourceUnused);
        // Decode values from their internal long representation
        value = new DirEntry[longList.value.length];
        for (int i = 0; i < value.length; i++)
            value[i] = new DirEntry(longList.value[i]);
        return ret;
    }

    @Override
    public int toBitStream(OutputBitStream outputBitStream, final long sourceUnused) throws IOException {
        // Values have already been encoded in the SwhLabel constructor
        return longList.toBitStream(outputBitStream, sourceUnused);
    }

    @Override
    public String wellKnownAttributeKey() {
        return key;
    }

    @Override
    public String[] attributeKeys() {
        return new String[]{key};
    }

    @Override
    public Class<?>[] attributeTypes() {
        return new Class[]{DirEntry[].class};
    }

    @Override
    public Object get(String s) {
        if (this.key.equals(s))
            return value;
        throw new IllegalArgumentException();
    }

    @Override
    public Object get() {
        return value;
    }

    @Override
    public Label copy() {
        return new SwhLabel(key, width, value.clone());
    }

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

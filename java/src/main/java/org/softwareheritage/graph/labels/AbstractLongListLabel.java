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

import it.unimi.dsi.big.webgraph.labelling.AbstractLabel;
import it.unimi.dsi.big.webgraph.labelling.Label;

import java.util.Arrays;

/**
 * An abstract (single-attribute) list-of-longs label.
 *
 * <p>
 * This class provides basic methods for a label holding a list of longs. Concrete implementations
 * may impose further requirements on the long.
 *
 * <p>
 * Implementing subclasses must provide constructors, {@link Label#copy()},
 * {@link Label#fromBitStream(it.unimi.dsi.io.InputBitStream, int)},
 * {@link Label#toBitStream(it.unimi.dsi.io.OutputBitStream, int)} and possibly override
 * {@link #toString()}.
 */

public abstract class AbstractLongListLabel extends AbstractLabel implements Label {
    /** The key of the attribute represented by this label. */
    protected final String key;
    /** The values of the attribute represented by this label. */
    public long[] value;

    /**
     * Creates an long label with given key and value.
     *
     * @param key the (only) key of this label.
     * @param value the value of this label.
     */
    public AbstractLongListLabel(String key, long[] value) {
        this.key = key;
        this.value = value;
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
        return new Class[]{long[].class};
    }

    @Override
    public Object get(String key) {
        if (this.key.equals(key))
            return value;
        throw new IllegalArgumentException();
    }

    @Override
    public Object get() {
        return value;
    }

    @Override
    public String toString() {
        return key + ":" + Arrays.toString(value);
    }

    @Override
    public boolean equals(Object x) {
        if (x instanceof AbstractLongListLabel)
            return Arrays.equals(value, ((AbstractLongListLabel) x).value);
        else
            return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}

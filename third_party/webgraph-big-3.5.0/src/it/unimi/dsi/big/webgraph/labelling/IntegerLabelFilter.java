package it.unimi.dsi.big.webgraph.labelling;

/*
 * Copyright (C) 2008-2017 Sebastiano Vigna
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

import it.unimi.dsi.big.webgraph.Transform.LabelledArcFilter;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

public class IntegerLabelFilter implements LabelledArcFilter {
	/** The values of the label that will be preserved. */
	private final IntOpenHashSet values;
	private final String key;

	public IntegerLabelFilter(final String key, int... value) {
		this.key = key;
		values = new IntOpenHashSet(value);
	}

	public IntegerLabelFilter(final String... keyAndvalues) {
		if (keyAndvalues.length == 0) throw new IllegalArgumentException("You must specificy a key name");
		this.key = keyAndvalues[0].length() == 0 ? null : keyAndvalues[0];
		values = new IntOpenHashSet(keyAndvalues.length);
		for(int i = 1; i < keyAndvalues.length; i++) values.add(Integer.parseInt(keyAndvalues[i]));
	}

	@Override
	public boolean accept(long i, long j, Label label) {
		return values.contains(key == null ? label.getInt() : label.getInt(key));
	}
}

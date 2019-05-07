package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2003-2017 Paolo Boldi
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

import static org.junit.Assert.assertEquals;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongIterator;

import java.util.Random;

import org.junit.Test;

public class MergedLongIteratorTest {

	public void testMerge(int n0, int n1) {
		long x0[] = new long[n0];
		long x1[] = new long[n1];
		int i;
		long p = 0;
		final Random random = new Random();

		// Generate
		for (i = 0; i < n0; i++) p = x0[i] = p + random.nextInt(10);
		p = 0;
		for (i = 0; i < n1; i++) p = x1[i] = p + random.nextInt(10);

		LongAVLTreeSet s0 = new LongAVLTreeSet(x0);
		LongAVLTreeSet s1 = new LongAVLTreeSet(x1);
		LongAVLTreeSet res = new LongAVLTreeSet(s0);
		res.addAll(s1);

		MergedLongIterator m = new MergedLongIterator(LazyLongIterators.lazy(s0.iterator()), LazyLongIterators.lazy(s1.iterator()));
		LongIterator it = res.iterator();

		long x;
		while ((x = m.nextLong()) != -1) assertEquals(it.nextLong(), x);
		assertEquals(Boolean.valueOf(it.hasNext()), Boolean.valueOf(m.nextLong() != -1));
	}

		@Test
	public void testMerge() {
		for(int i = 0; i < 10; i++) {
			testMerge(i, i);
			testMerge(i, i + 1);
			testMerge(i, i * 2);
		}
	}
}

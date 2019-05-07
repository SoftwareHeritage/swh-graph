package it.unimi.dsi.law.util;

/*
 * Copyright (C) 2011-2019 Sebastiano Vigna
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

import org.junit.Test;


//RELEASE-STATUS: DIST

public class KahanSummationTest {
	@Test
	public void testSum() {
		KahanSummation sum = new KahanSummation();
		sum.add(1);
		sum.add(2);
		sum.add(3);
		assertEquals(6, sum.value(), 0);
	}

	@Test
	public void testDifficult() {
		KahanSummation sum = new KahanSummation();
		sum.add(Double.MIN_NORMAL);
		sum.add(Double.MIN_NORMAL);
		sum.add(-Double.MIN_NORMAL);
		assertEquals(Double.MIN_NORMAL, sum.value(), 0);
	}
}

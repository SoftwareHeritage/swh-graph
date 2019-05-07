package it.unimi.dsi.law.warc.util;

/*
 * Copyright (C) 2012-2019 Paolo Boldi, Massimo Santini and Sebastiano Vigna
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

public class ByteArrayCharSequenceTest {

	@Test
	public void test() {
		assertEquals(new ByteArrayCharSequence(new byte[] { 48, 49 }).toString(), "01");
		assertEquals(new ByteArrayCharSequence(new byte[] { 48, 49, 50, 51 }, 1, 2).toString(), "12");

		assertEquals(new ByteArrayCharSequence(new byte[] { 48, 49 }).hashCode(), "01".hashCode());
		assertEquals(new ByteArrayCharSequence(new byte[] { 48, 49, 50, 51 }, 1, 2).hashCode(), "12".hashCode());
	}
}

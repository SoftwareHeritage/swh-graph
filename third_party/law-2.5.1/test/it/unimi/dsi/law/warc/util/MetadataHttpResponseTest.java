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

public class MetadataHttpResponseTest {

	@Test
	public void testHeaderMap() {
		MetadataHttpResponse.HeaderMap headerMap = new MetadataHttpResponse.HeaderMap();

		headerMap.put("header0", "value0a");
		headerMap.put("header0", "value0b");
		headerMap.put("header1", "value1");
		headerMap.put("header2", "value2");

		assertEquals("value0a,value0b", headerMap.get("header0"));
		assertEquals("value1", headerMap.get("header1"));
		assertEquals("value2", headerMap.get("header2"));
		assertEquals(null, headerMap.get("doesNotExist"));
		assertEquals(3, headerMap.size());
	}
}

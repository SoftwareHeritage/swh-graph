package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2010-2017 Sebastiano Vigna
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
import static org.junit.Assert.assertArrayEquals;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.longs.LongIterators;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.StringReader;
import java.net.URISyntaxException;

import org.junit.Test;

public class BuildHostMapTest extends WebGraphTestCase {

	@Test
	public void testSimpleNoLogger() throws IOException, URISyntaxException {
		BufferedReader br = new BufferedReader(new StringReader("http://a/b\nhttp://c\nhttp://a.b:81/\nhttp://c/c\nhttp://a:80/\nhttps://a/\nhttps://a.b"));
		FastByteArrayOutputStream mapFbaos = new FastByteArrayOutputStream();
		FastByteArrayOutputStream countFbaos = new FastByteArrayOutputStream();
		FastByteArrayOutputStream hostsStream = new FastByteArrayOutputStream();
		PrintStream hosts = new PrintStream(hostsStream);
		DataOutputStream mapDos = new DataOutputStream(mapFbaos);
		DataOutputStream countDos = new DataOutputStream(countFbaos);
		BuildHostMap.run(br, hosts, mapDos, countDos, false, null);
		mapDos.close();
		hosts.close();
		DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(mapFbaos.array, 0, mapFbaos.length));
		assertEquals(0, dis.readLong());
		assertEquals(1, dis.readLong());
		assertEquals(2, dis.readLong());
		assertEquals(1, dis.readLong());
		assertEquals(0, dis.readLong());
		assertEquals(0, dis.readLong());
		assertEquals(2, dis.readLong());
		assertEquals(0, dis.available());
		dis.close();
		BufferedReader hostsIn = new BufferedReader(new InputStreamReader(new FastByteArrayInputStream(hostsStream.array, 0, hostsStream.length)));
		assertEquals("a", hostsIn.readLine());
		assertEquals("c", hostsIn.readLine());
		assertEquals("a.b", hostsIn.readLine());
		assertEquals(null, hostsIn.readLine());
		hostsIn.close();
		assertArrayEquals(new long[] { 3, 2, 2 }, LongIterators.unwrap(BinIO.asLongIterator(new DataInputStream(new FastByteArrayInputStream(countFbaos.array, 0, countFbaos.length)))));
	}

	@Test
	public void testSimpleLogger() throws IOException, URISyntaxException {
		BufferedReader br = new BufferedReader(new StringReader("http://a/b\nhttp://c\nhttp://a.b/\nhttp://c/c\nhttp://a/\nhttps://a/\nhttps://a.b"));
		FastByteArrayOutputStream mapFbaos = new FastByteArrayOutputStream();
		FastByteArrayOutputStream countFbaos = new FastByteArrayOutputStream();
		FastByteArrayOutputStream hostsStream = new FastByteArrayOutputStream();
		PrintStream hosts = new PrintStream(hostsStream);
		DataOutputStream mapDos = new DataOutputStream(mapFbaos);
		DataOutputStream countDos = new DataOutputStream(countFbaos);
		BuildHostMap.run(br, hosts, mapDos, countDos, false, new ProgressLogger());
		mapDos.close();
		hosts.close();
		DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(mapFbaos.array, 0, mapFbaos.length));
		assertEquals(0, dis.readLong());
		assertEquals(1, dis.readLong());
		assertEquals(2, dis.readLong());
		assertEquals(1, dis.readLong());
		assertEquals(0, dis.readLong());
		assertEquals(0, dis.readLong());
		assertEquals(2, dis.readLong());
		assertEquals(0, dis.available());
		dis.close();
		BufferedReader hostsIn = new BufferedReader(new InputStreamReader(new FastByteArrayInputStream(hostsStream.array, 0, hostsStream.length)));
		assertEquals("a", hostsIn.readLine());
		assertEquals("c", hostsIn.readLine());
		assertEquals("a.b", hostsIn.readLine());
		assertEquals(null, hostsIn.readLine());
		hostsIn.close();
		assertArrayEquals(new long[] { 3, 2, 2 }, LongIterators.unwrap(BinIO.asLongIterator(new DataInputStream(new FastByteArrayInputStream(countFbaos.array, 0, countFbaos.length)))));
	}

	@Test
	public void testTopPrivateDomainNoLogger() throws IOException, URISyntaxException {
		BufferedReader br = new BufferedReader(new StringReader("http://b.a.co.uk/b\nhttp://c.a.co.uk\nhttp://a.b.co.uk\nhttp://159.149.130.49/"));
		FastByteArrayOutputStream mapFbaos = new FastByteArrayOutputStream();
		FastByteArrayOutputStream countFbaos = new FastByteArrayOutputStream();
		FastByteArrayOutputStream hostsStream = new FastByteArrayOutputStream();
		PrintStream hosts = new PrintStream(hostsStream);
		DataOutputStream mapDos = new DataOutputStream(mapFbaos);
		DataOutputStream countDos = new DataOutputStream(countFbaos);
		BuildHostMap.run(br, hosts, mapDos, countDos, true, null);
		mapDos.close();
		hosts.close();
		DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(mapFbaos.array, 0, mapFbaos.length));
		assertEquals(0, dis.readLong());
		assertEquals(0, dis.readLong());
		assertEquals(1, dis.readLong());
		assertEquals(2, dis.readLong());
		assertEquals(0, dis.available());
		dis.close();
		BufferedReader hostsIn = new BufferedReader(new InputStreamReader(new FastByteArrayInputStream(hostsStream.array, 0, hostsStream.length)));
		assertEquals("a.co.uk", hostsIn.readLine());
		assertEquals("b.co.uk", hostsIn.readLine());
		assertEquals("159.149.130.49", hostsIn.readLine());
		assertEquals(null, hostsIn.readLine());
		hostsIn.close();
		assertArrayEquals(new long[] { 2, 1, 1 }, LongIterators.unwrap(BinIO.asLongIterator(new DataInputStream(new FastByteArrayInputStream(countFbaos.array, 0, countFbaos.length)))));
	}

	@Test(expected=IllegalArgumentException.class)
	public void testMalformed() throws IOException, URISyntaxException {
		BufferedReader br = new BufferedReader(new StringReader("http://a/b\nhttp://c\nhttp//a.b/\nhttp://c/c\nhttp://a/\nhttps://a/\nhttps://a.b"));
		FastByteArrayOutputStream mapFbaos = new FastByteArrayOutputStream();
		FastByteArrayOutputStream countFbaos = new FastByteArrayOutputStream();
		FastByteArrayOutputStream hostsStream = new FastByteArrayOutputStream();
		PrintStream hosts = new PrintStream(hostsStream);
		DataOutputStream mapDos = new DataOutputStream(mapFbaos);
		DataOutputStream countDos = new DataOutputStream(countFbaos);
		BuildHostMap.run(br, hosts, mapDos, countDos, false, null);
	}
}

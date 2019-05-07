package it.unimi.dsi.big.webgraph;

/*
 * Copyright (C) 2007-2017 Sebastiano Vigna
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
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.webgraph.ArrayListMutableGraph;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class ShiftedByOneArcListASCIIGraphTest extends WebGraphTestCase {

	@Test
	public void testLoadOnce() throws UnsupportedEncodingException, IOException {

		ArcListASCIIGraph g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("1 3\n1 2\n2 1\n2 3\n3 1\n3 2".getBytes("ASCII")));
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("3 1\n3 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,0},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("2 3".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{1,2}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("3 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		g = ShiftedByOneArcListASCIIGraph.loadOnce(new FastByteArrayInputStream("1 2\n3 2".getBytes("ASCII")));
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
	}

	@Test
	public void testLoad() throws UnsupportedEncodingException, IOException {
		File file = File.createTempFile(ShiftedByOneArcListASCIIGraphTest.class.getSimpleName(), ".txt");
		file.deleteOnExit();
		FileUtils.writeStringToFile(file, "1 3\n1 2\n2 1\n2 3\n3 1\n3 2", StandardCharsets.US_ASCII);
		ImmutableGraph g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		FileUtils.writeStringToFile(file, "3 1\n3 2", StandardCharsets.US_ASCII);
		g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,0},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		FileUtils.writeStringToFile(file, "2 3", StandardCharsets.US_ASCII);
		g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{1,2}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		FileUtils.writeStringToFile(file, "3 2", StandardCharsets.US_ASCII);
		g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		FileUtils.writeStringToFile(file, "1 2\n3 2", StandardCharsets.US_ASCII);
		g = ShiftedByOneArcListASCIIGraph.load(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
	}

	@Test
	public void testLoadMapped() throws IOException {
		File file = File.createTempFile(ShiftedByOneArcListASCIIGraphTest.class.getSimpleName(), ".txt");
		file.deleteOnExit();
		FileUtils.writeStringToFile(file, "1 3\n1 2\n2 1\n2 3\n3 1\n3 2", StandardCharsets.US_ASCII);
		ImmutableGraph g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(ArrayListMutableGraph.newCompleteGraph(3, false).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		FileUtils.writeStringToFile(file, "3 1\n3 2", StandardCharsets.US_ASCII);
		g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,0},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		FileUtils.writeStringToFile(file, "2 3", StandardCharsets.US_ASCII);
		g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{1,2}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		FileUtils.writeStringToFile(file, "3 2", StandardCharsets.US_ASCII);
		g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());

		FileUtils.writeStringToFile(file, "1 2\n3 2", StandardCharsets.US_ASCII);
		g = ShiftedByOneArcListASCIIGraph.loadMapped(file.toString());
		assertEquals(new ArrayListMutableGraph(3, new int[][] {{0,1},{2,1}}).immutableView(), new ArrayListMutableGraph(ImmutableGraph.wrap(g)).immutableView());
	}
}

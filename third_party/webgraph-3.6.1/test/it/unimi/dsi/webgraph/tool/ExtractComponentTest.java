package it.unimi.dsi.webgraph.tool;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.webgraph.tool.ExtractComponent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.martiansoftware.jsap.JSAPException;

public class ExtractComponentTest {

	@Test
	public void test() throws IOException, JSAPException {
		File componentsFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-components");
		componentsFile.deleteOnExit();
		File mapFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-map");
		mapFile.deleteOnExit();
		File inIds = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-inIds");
		inIds.deleteOnExit();
		File outIds = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-outIds");
		outIds.deleteOnExit();

		BinIO.storeInts(new int[] { 1, 0, 1, 0, 0, 2, 1, 0 }, componentsFile);
		IOUtils.writeLines(Arrays.asList(new String[] { "a", "b", "c", "d", "e", "f", "g", "h" }), null, new FileOutputStream(inIds), Charsets.UTF_8);
		ExtractComponent.main(new String[] { componentsFile.toString(), mapFile.toString(), inIds.toString(), outIds.toString() });

		assertArrayEquals(new int[] { -1, 0, -1, 1, 2, -1, -1, 3 }, BinIO.loadInts(mapFile));
		assertEquals(Arrays.asList(new String[] { "b", "d", "e", "h" }), IOUtils.readLines(new FileInputStream(outIds), Charsets.UTF_8));

		componentsFile.delete();
		mapFile.delete();
		inIds.delete();
		outIds.delete();
	}

	@Test
	public void testNoIds() throws IOException, JSAPException {
		File componentsFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-components");
		componentsFile.deleteOnExit();
		File mapFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-map");
		mapFile.deleteOnExit();

		BinIO.storeInts(new int[] { 1, 0, 1, 0, 0, 2, 1, 0 }, componentsFile);
		ExtractComponent.main(new String[] { componentsFile.toString(), mapFile.toString() });

		assertArrayEquals(new int[] { -1, 0, -1, 1, 2, -1, -1, 3 }, BinIO.loadInts(mapFile));

		componentsFile.delete();
		mapFile.delete();
	}

	@Test(expected=IllegalArgumentException.class)
	public void testDifferentLengths() throws IOException, JSAPException {
		File componentsFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-components");
		componentsFile.deleteOnExit();
		File mapFile = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-map");
		mapFile.deleteOnExit();
		File inIds = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-inIds");
		inIds.deleteOnExit();
		File outIds = File.createTempFile(ExtractComponentTest.class.getSimpleName() + "-", "-outIds");
		outIds.deleteOnExit();

		BinIO.storeInts(new int[] { 1, 0, 1, 0, 0, 2, 1, 0 }, componentsFile);
		IOUtils.writeLines(Arrays.asList(new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" }), null, new FileOutputStream(inIds), Charsets.UTF_8);
		ExtractComponent.main(new String[] { componentsFile.toString(), mapFile.toString(), inIds.toString(), outIds.toString() });
	}
}

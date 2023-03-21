/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

import com.github.luben.zstd.ZstdInputStream;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterators;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class GraphTest {
    static SwhBidirectionalGraph graph;

    final protected String TEST_ORIGIN_ID = "swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054";
    final protected String TEST_ORIGIN_ID2 = "swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165";

    @BeforeAll
    public static void setUp() throws IOException {
        graph = SwhBidirectionalGraph.loadLabelled(getGraphPath().toString());
    }

    public static Path getGraphPath() {
        return Paths.get("..", "swh", "graph", "example_dataset", "compressed", "example");
    }

    public static SwhBidirectionalGraph getGraph() {
        return graph;
    }

    public static SWHID fakeSWHID(String type, int num) {
        return new SWHID(String.format("swh:1:%s:%040d", type, num));
    }

    public static <T> void assertEqualsAnyOrder(Collection<T> expected, Collection<T> actual) {
        ArrayList<T> expectedList = new ArrayList<>(expected);
        ArrayList<T> actualList = new ArrayList<>(actual);
        expectedList.sort(Comparator.comparing(Object::toString));
        actualList.sort(Comparator.comparing(Object::toString));
        assertEquals(expectedList, actualList);
    }

    public static <T> void assertContainsAll(Collection<T> expected, Collection<T> actual) {
        ArrayList<T> expectedList = new ArrayList<>(expected);
        ArrayList<T> actualList = new ArrayList<>(actual);
        expectedList.sort(Comparator.comparing(Object::toString));
        Iterator<T> expectedIterator = expectedList.iterator();

        actualList.sort(Comparator.comparing(Object::toString));

        for (T actualItem : actualList) {
            boolean found = false;
            while (expectedIterator.hasNext()) {
                if (expectedIterator.next().equals(actualItem)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                // TODO: better message when actualItem is present twice in actualList,
                // but only once in expectedList
                fail(String.format("%s not found in %s", actualItem, expectedList));
            }
        }
    }

    public static <T> void assertLength(int expected, Collection<T> actual) {
        assertEquals(String.format("Size of collection %s:", actual), expected, actual.size());
    }

    public static ArrayList<Long> lazyLongIteratorToList(LazyLongIterator input) {
        ArrayList<Long> inputList = new ArrayList<>();
        Iterator<Long> inputIt = LazyLongIterators.eager(input);
        inputIt.forEachRemaining(inputList::add);
        return inputList;
    }

    public static String[] readZstFile(Path zstFile) throws IOException {
        ZstdInputStream zis = new ZstdInputStream(new FileInputStream(zstFile.toFile()));
        return (new String(zis.readAllBytes())).split("\n");
    }
}

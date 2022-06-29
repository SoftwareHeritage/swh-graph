/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.softwareheritage.graph.GraphTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

public class ExtractPersonsTest extends GraphTest {
    private static class FakeORCDataset extends ORCGraphDataset {
        private static class FakeSwhOrcTable extends ORCGraphDataset.SwhOrcTable {
            private final String tableName;

            public FakeSwhOrcTable(String tableName) {
                this.tableName = tableName;
            }

            @Override
            public void readBytes64Column(String longColumn, BytesCallback cb) throws IOException {
                if (tableName.equals("revision") && longColumn.equals("author")) {
                    cb.onBytes(fakeSWHID("rev", 1).toBytes(), "rev_author_1".getBytes());
                    cb.onBytes(fakeSWHID("rev", 2).toBytes(), "rev_author_1".getBytes());
                    cb.onBytes(fakeSWHID("rev", 3).toBytes(), "rev_author_2".getBytes());
                    cb.onBytes(fakeSWHID("rev", 4).toBytes(), "rev_author_1".getBytes());
                    cb.onBytes(fakeSWHID("rev", 5).toBytes(), "rev_author_3".getBytes());
                } else if (tableName.equals("revision") && longColumn.equals("committer")) {
                    cb.onBytes(fakeSWHID("rev", 1).toBytes(), "rev_committer_1".getBytes());
                    cb.onBytes(fakeSWHID("rev", 2).toBytes(), "rev_committer_1".getBytes());
                    cb.onBytes(fakeSWHID("rev", 3).toBytes(), "rev_committer_2".getBytes());
                    cb.onBytes(fakeSWHID("rev", 4).toBytes(), "rev_author_2".getBytes());
                    cb.onBytes(fakeSWHID("rev", 5).toBytes(), "rev_author_1".getBytes());
                    cb.onBytes(fakeSWHID("rev", 6).toBytes(), "rev_committer_1".getBytes());
                } else if (tableName.equals("release") && longColumn.equals("author")) {
                    cb.onBytes(fakeSWHID("rel", 1).toBytes(), "rel_committer_1".getBytes());
                    cb.onBytes(fakeSWHID("rel", 2).toBytes(), "rel_committer_1".getBytes());
                    cb.onBytes(fakeSWHID("rel", 3).toBytes(), "rel_committer_2".getBytes());
                    cb.onBytes(fakeSWHID("rel", 4).toBytes(), "rev_author_2".getBytes());
                    cb.onBytes(fakeSWHID("rel", 5).toBytes(), "rev_author_1".getBytes());
                    cb.onBytes(fakeSWHID("rel", 6).toBytes(), "rev_committer_1".getBytes());
                    cb.onBytes(fakeSWHID("rel", 7).toBytes(), "rel_committer_1".getBytes());
                } else {
                    throw new RuntimeException("Unknown table/column: " + tableName + "/" + longColumn);
                }
            }
        }

        public SwhOrcTable getTable(String tableName) {
            return new FakeSwhOrcTable(tableName);
        }
    }

    @Test
    public void testExtractPersons(@TempDir Path outputDir, @TempDir Path sortTmpDir)
            throws IOException, InterruptedException {

        FakeORCDataset fakeORCDataset = new FakeORCDataset();
        ExtractPersons.extractPersons(fakeORCDataset, outputDir.toString() + "/graph", "2M", sortTmpDir.toString());

        ArrayList<String> expectedPersons = new ArrayList<>(Arrays.asList("rev_author_1", "rev_author_2",
                "rev_author_3", "rev_committer_1", "rev_committer_2", "rel_committer_1", "rel_committer_2"));

        // Check count files
        Long personsCount = Long.parseLong(Files.readString(outputDir.resolve("graph.persons.count.txt")).strip());
        Assertions.assertEquals(expectedPersons.size(), personsCount);

        // Check persons
        expectedPersons.sort(String::compareTo);
        String[] personLines = readZstFile(outputDir.resolve("graph.persons.csv.zst"));
        Assertions.assertArrayEquals(expectedPersons.toArray(new String[0]), personLines);
    }
}

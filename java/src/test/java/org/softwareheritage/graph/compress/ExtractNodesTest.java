/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeSet;

public class ExtractNodesTest extends GraphTest {
    /** Generate a fake SWHID for a given node type and numeric ID */
    private static byte[] f(String type, int id) {
        String hash = new String(DigestUtils.sha1Hex(type + id).getBytes());
        return String.format("swh:1:%s:%s", type, hash).getBytes();
    }

    static class FakeDataset implements GraphDataset {
        @Override
        public void readEdges(NodeCallback nodeCb, EdgeCallback edgeCb) throws IOException {
            // For each node type, write nodes {1..4} as present in the graph
            for (SwhType type : SwhType.values()) {
                for (int i = 1; i <= 4; i++) {
                    byte[] node = f(type.toString().toLowerCase(), i);
                    nodeCb.onNode(node);
                }
            }

            edgeCb.onEdge(f("ori", 1), f("snp", 1), null, -1);
            edgeCb.onEdge(f("ori", 2), f("snp", 2), null, -1);
            edgeCb.onEdge(f("ori", 3), f("snp", 3), null, -1);
            edgeCb.onEdge(f("ori", 4), f("snp", 404), null, -1);

            edgeCb.onEdge(f("snp", 1), f("rev", 1), "dup1".getBytes(), -1);
            edgeCb.onEdge(f("snp", 1), f("rev", 1), "dup2".getBytes(), -1);
            edgeCb.onEdge(f("snp", 3), f("cnt", 1), "c1".getBytes(), -1);
            edgeCb.onEdge(f("snp", 4), f("rel", 1), "r1".getBytes(), -1);

            edgeCb.onEdge(f("rel", 1), f("rel", 2), null, -1);
            edgeCb.onEdge(f("rel", 2), f("rev", 1), null, -1);
            edgeCb.onEdge(f("rel", 3), f("rev", 2), null, -1);
            edgeCb.onEdge(f("rel", 4), f("dir", 1), null, -1);

            edgeCb.onEdge(f("rev", 1), f("rev", 1), null, -1);
            edgeCb.onEdge(f("rev", 1), f("rev", 1), null, -1);
            edgeCb.onEdge(f("rev", 1), f("rev", 2), null, -1);
            edgeCb.onEdge(f("rev", 2), f("rev", 404), null, -1);
            edgeCb.onEdge(f("rev", 3), f("rev", 2), null, -1);
            edgeCb.onEdge(f("rev", 4), f("dir", 1), null, -1);

            edgeCb.onEdge(f("dir", 1), f("cnt", 1), "c1".getBytes(), 42);
            edgeCb.onEdge(f("dir", 1), f("dir", 1), "d1".getBytes(), 1337);
            edgeCb.onEdge(f("dir", 1), f("rev", 1), "r1".getBytes(), 0);
        }
    }

    @Test
    public void testExtractNodes(@TempDir Path outputDir, @TempDir Path sortTmpDir)
            throws IOException, InterruptedException {
        FakeDataset dataset = new FakeDataset();
        ExtractNodes.extractNodes(dataset, outputDir.toString() + "/graph", "2M", sortTmpDir.toFile());

        // Check count files
        Long nodeCount = Long.parseLong(Files.readString(outputDir.resolve("graph.nodes.count.txt")).strip());
        Long edgeCount = Long.parseLong(Files.readString(outputDir.resolve("graph.edges.count.txt")).strip());
        Long labelCount = Long.parseLong(Files.readString(outputDir.resolve("graph.labels.count.txt")).strip());
        Assertions.assertEquals(26L, nodeCount);
        Assertions.assertEquals(21L, edgeCount);
        Assertions.assertEquals(5L, labelCount);

        // Check stat files
        List<String> nodeStats = Files.readAllLines(outputDir.resolve("graph.nodes.stats.txt"));
        List<String> edgeStats = Files.readAllLines(outputDir.resolve("graph.edges.stats.txt"));
        Assertions.assertEquals(nodeStats, List.of("cnt 4", "dir 4", "ori 4", "rel 4", "rev 5", "snp 5"));
        Assertions.assertEquals(edgeStats, List.of("dir:cnt 1", "dir:dir 1", "dir:rev 1", "ori:snp 4", "rel:dir 1",
                "rel:rel 1", "rel:rev 2", "rev:dir 1", "rev:rev 5", "snp:cnt 1", "snp:rel 1", "snp:rev 2"));

        // Build ordered set of expected node IDs
        TreeSet<String> expectedNodes = new TreeSet<>();
        for (SwhType type : SwhType.values()) {
            for (int i = 1; i <= 4; i++) {
                byte[] node = f(type.toString().toLowerCase(), i);
                expectedNodes.add(new String(node));
            }
        }
        expectedNodes.add(new String(f("snp", 404)));
        expectedNodes.add(new String(f("rev", 404)));
        String[] nodeLines = readZstFile(outputDir.resolve("graph.nodes.csv.zst"));
        Assertions.assertArrayEquals(expectedNodes.toArray(new String[0]), nodeLines);

        // Build ordered set of expected label IDs
        TreeSet<String> expectedLabels = new TreeSet<>();
        expectedLabels.add("dup1");
        expectedLabels.add("dup2");
        expectedLabels.add("c1");
        expectedLabels.add("r1");
        expectedLabels.add("d1");
        String[] labelLines = readZstFile(outputDir.resolve("graph.labels.csv.zst"));
        Assertions.assertArrayEquals(expectedLabels.toArray(new String[0]), labelLines);
    }
}

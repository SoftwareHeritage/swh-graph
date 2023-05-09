/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

/* For each origin and each contributor, outputs a line "origin_id,contributor_id",
 * if that contributor contributed to the origin.
 *
 * A .csv table containing "origin_id,origin_url_base64" is also written
 * to the given path.
 *
 * This takes the output of TopoSort on stdin.
 *
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import org.softwareheritage.graph.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class ListOriginContributors {
    /*
     * For nodes with a single ancestor, reuses the ancestor's set of contributors instead of copying,
     * when that ancestor has no more pending successors.
     */
    private static boolean optimizeReuse = true;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.FindEarliestRevision <path/to/graph> <path/to/origin_urls.csv>");
            System.exit(1);
        }
        String graphBasename = args[0];
        FileWriter originsFileWriter = new FileWriter(args[1]);
        CSVPrinter originsCsvPrinter = new CSVPrinter(originsFileWriter, CSVFormat.RFC4180);

        System.err.println("Loading graph " + graphBasename + " ...");
        SwhUnidirectionalGraph underlyingGraph = SwhUnidirectionalGraph.loadMapped(graphBasename);
        System.err.println("Loading person ids");
        underlyingGraph.loadPersonIds();
        System.err.println("Loading messages");
        underlyingGraph.loadMessages();
        System.err.println("Selecting subgraph.");
        AllowedNodes allowedNodeTypes = new AllowedNodes("rev,rel,snp,ori");
        System.err.println("Graph loaded.");

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        CSVPrinter csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));
        CSVParser csvParser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        /* Map each node id to its set of contributor person ids */
        HashMap<Long, HashSet<Long>> contributors = new HashMap<>();

        /*
         * For each node it, counts its number of direct successors that still need to be handled
         */
        HashMap<Long, Long> pendingSuccessors = new HashMap<>();

        csvPrinter.printRecord("origin_id", "contributor_id");
        originsCsvPrinter.printRecord("origin_id", "origin_url_base64");
        boolean seenHeader = false;
        for (CSVRecord record : csvParser) {
            if (!seenHeader) {
                if (!Arrays.deepEquals(record.values(),
                        new String[]{"SWHID", "ancestors", "successors", "sample_ancestor1", "sample_ancestor2"})) {
                    System.err.format("Unexpected header: %s\n", record);
                    System.exit(2);
                }
                seenHeader = true;
                continue;
            }
            SWHID nodeSWHID = new SWHID(record.get(0));
            long nodeId = underlyingGraph.getNodeId(nodeSWHID);
            long ancestorCount = Long.parseLong(record.get(1));
            long successorCount = Long.parseLong(record.get(2));
            String sampleAncestor1SWHID = record.get(3);

            HashSet<Long> nodeContributors;
            boolean reuseAncestorSet = optimizeReuse && (ancestorCount == 1);

            if (reuseAncestorSet) {
                long ancestorNodeId = underlyingGraph.getNodeId(new SWHID(sampleAncestor1SWHID));
                if (pendingSuccessors.get(ancestorNodeId) == 1) {
                    nodeContributors = contributors.remove(ancestorNodeId);
                    pendingSuccessors.remove(ancestorNodeId);
                } else {
                    /* Ancestor is not yet ready to be popped */
                    pendingSuccessors.put(ancestorNodeId, pendingSuccessors.get(ancestorNodeId) - 1);
                    nodeContributors = new HashSet<>();
                }
            } else {
                nodeContributors = new HashSet<>();
            }

            Long personId;
            if (nodeSWHID.getType() == SwhType.REV) {
                personId = underlyingGraph.getAuthorId(nodeId);
                if (personId != null) {
                    nodeContributors.add(personId);
                }
                personId = underlyingGraph.getCommitterId(nodeId);
                if (personId != null) {
                    nodeContributors.add(personId);
                }
            } else if (nodeSWHID.getType() == SwhType.REL) {
                personId = underlyingGraph.getAuthorId(nodeId);
                if (personId != null) {
                    nodeContributors.add(personId);
                }
            }

            if (!reuseAncestorSet) {
                long computedAncestorCount = 0;
                LazyLongIterator it = underlyingGraph.successors(nodeId);
                for (long ancestorNodeId; (ancestorNodeId = it.nextLong()) != -1;) {
                    if (!allowedNodeTypes.isAllowed(underlyingGraph.getNodeType(ancestorNodeId))) {
                        continue;
                    }
                    computedAncestorCount++;
                    if (pendingSuccessors.get(ancestorNodeId) == 1) {
                        /*
                         * If this node is the last unhandled successor of the ancestor; pop the ancestor information,
                         * as we won't need it anymore
                         */
                        pendingSuccessors.remove(ancestorNodeId);
                        nodeContributors.addAll(contributors.remove(ancestorNodeId));
                    } else {
                        /*
                         * The ancestor has remaining successors to handle; decrement the counter and copy its set of
                         * contributors to the current set
                         */
                        pendingSuccessors.put(ancestorNodeId, pendingSuccessors.get(ancestorNodeId) - 1);
                        nodeContributors.addAll(contributors.get(ancestorNodeId));
                    }
                }

                if (ancestorCount != computedAncestorCount) {
                    System.err.format("Mismatched ancestor count: expected %d, found %d", ancestorCount,
                            computedAncestorCount);
                    System.exit(2);
                }
            }

            if (nodeSWHID.getType() == SwhType.ORI) {
                nodeContributors.forEach((contributorId) -> {
                    try {
                        csvPrinter.printRecord(nodeId, contributorId);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
                byte[] url = underlyingGraph.getMessageBase64(nodeId);
                if (url != null) {
                    originsCsvPrinter.printRecord(nodeId, new String(url));
                }
            }

            if (successorCount > 0) {
                /*
                 * If the node has any successor, store its set of contributors for later
                 */
                contributors.put(nodeId, nodeContributors);
                pendingSuccessors.put(nodeId, successorCount);
            }
        }

        csvPrinter.flush();
        bufferedStdout.flush();
        originsCsvPrinter.flush();
        originsFileWriter.flush();
    }
}

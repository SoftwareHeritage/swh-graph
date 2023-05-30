/*
 * Copyright (c) 2022-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import com.github.luben.zstd.ZstdOutputStream;
import com.martiansoftware.jsap.*;
import org.softwareheritage.graph.utils.Sort;
import org.softwareheritage.graph.AllowedNodes;
import org.softwareheritage.graph.SwhType;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Read a graph dataset and extract all the unique authors it contains.
 *
 * <p>
 * This class reads the revision and release tables of the graph dataset, and uses
 * <code>sort -u</code> to extract the set of all the unique persons (name + email, potentially
 * pseudonymized) and store them in a file.
 * </p>
 */
public class ExtractPersons {
    private static JSAPResult parseArgs(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ComposePermutations.class.getName(), "", new Parameter[]{
                    new UnflaggedOption("dataset", JSAP.STRING_PARSER, JSAP.REQUIRED, "Path to the ORC dataset"),
                    new UnflaggedOption("outputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED,
                            "Basename of the output files"),

                    new FlaggedOption("sortBufferSize", JSAP.STRING_PARSER, "30%", JSAP.NOT_REQUIRED, 'S',
                            "sort-buffer-size", "Size of the memory buffer used by sort"),
                    new FlaggedOption("sortTmpDir", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'T', "temp-dir",
                            "Path to the temporary directory used by sort"),
                    new FlaggedOption("allowedNodeTypes", JSAP.STRING_PARSER, "*", JSAP.NOT_REQUIRED, 'N',
                            "allowed-node-types",
                            "Node types to include in the graph, eg. 'ori,snp,rel,rev' to exclude directories and contents"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            System.err.println("Usage error: " + e.getMessage());
            System.exit(1);
        }
        return config;
    }

    private static void processAuthorColumn(ORCGraphDataset.SwhOrcTable table, String columnName, OutputStream stream)
            throws IOException {
        table.readBytes64Column(columnName, (swhid, personBase64) -> {
            stream.write(personBase64);
            stream.write('\n');
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JSAPResult parsedArgs = parseArgs(args);
        String datasetPath = parsedArgs.getString("dataset");
        String outputBasename = parsedArgs.getString("outputBasename");

        String sortBufferSize = parsedArgs.getString("sortBufferSize");
        String sortTmpDir = parsedArgs.getString("sortTmpDir", null);
        AllowedNodes allowedNodeTypes = new AllowedNodes(parsedArgs.getString("allowedNodeTypes"));

        ORCGraphDataset dataset = new ORCGraphDataset(datasetPath, allowedNodeTypes);

        extractPersons(dataset, outputBasename, sortBufferSize, sortTmpDir);
    }

    public static void extractPersons(ORCGraphDataset dataset, String outputBasename, String sortBufferSize,
            String sortTmpDir) throws IOException, InterruptedException {
        (new File(sortTmpDir)).mkdirs();

        // Spawn person sorting process
        Process personSort = Sort.spawnSort(sortBufferSize, sortTmpDir);
        BufferedOutputStream personSortStdin = new BufferedOutputStream(personSort.getOutputStream());
        BufferedInputStream personSortStdout = new BufferedInputStream(personSort.getInputStream());
        OutputStream personsFileOutputStream = new ZstdOutputStream(
                new BufferedOutputStream(new FileOutputStream(outputBasename + ".persons.csv.zst")));
        PersonsOutputThread personsOutputThread = new PersonsOutputThread(personSortStdout, personsFileOutputStream);
        personsOutputThread.start();

        if (dataset.allowedNodeTypes.isAllowed(SwhType.REL)) {
            processAuthorColumn(dataset.getTable("release"), "author", personSortStdin);
        }
        if (dataset.allowedNodeTypes.isAllowed(SwhType.REV)) {
            processAuthorColumn(dataset.getTable("revision"), "author", personSortStdin);
            processAuthorColumn(dataset.getTable("revision"), "committer", personSortStdin);
        }

        // Wait for sorting processes to finish
        personSortStdin.close();
        personSort.waitFor();
        personsOutputThread.join();

        // Write person count statistics
        printPersonsCounts(outputBasename, personsOutputThread.getPersonCount());
    }

    private static void printPersonsCounts(String basename, long labelCount) throws IOException {
        PrintWriter nodeCountWriter = new PrintWriter(basename + ".persons.count.txt");
        nodeCountWriter.println(labelCount);
        nodeCountWriter.close();
    }

    private static class PersonsOutputThread extends Thread {
        private final InputStream sortedPersonsStream;
        private final OutputStream personsOutputStream;

        private long personCount = 0;

        PersonsOutputThread(InputStream sortedNodesStream, OutputStream nodesOutputStream) {
            this.sortedPersonsStream = sortedNodesStream;
            this.personsOutputStream = nodesOutputStream;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(sortedPersonsStream, StandardCharsets.UTF_8));
            try {
                /*
                 * Workaround for <https://github.com/luben/zstd-jni/issues/249>, which happens when
                 * allowedNodeTypes does not contain REV or REL)
                 */
                personsOutputStream.write(new byte[]{});

                String line;
                while ((line = reader.readLine()) != null) {
                    personsOutputStream.write(line.getBytes(StandardCharsets.UTF_8));
                    personsOutputStream.write('\n');
                    personCount++;
                }
                personsOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public long getPersonCount() {
            return personCount;
        }
    }
}

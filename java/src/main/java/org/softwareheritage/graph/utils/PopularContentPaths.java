/*
 * Copyright (c) 2020-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import com.martiansoftware.jsap.*;
import org.softwareheritage.graph.*;
import org.softwareheritage.graph.labels.DirEntry;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Reads a list of content and directory SWHIDs, and for each of them, returns
 * their most popular path among their parents, up to the maximum given depth.
 *
 * Contents under 3 bytes are skipped, as they are too long to process (many references
 * to them) and not interesting.
 *
 * Syntax:
 *
 * <code>java org.softwareheritage.graph.utils.PopularContentPaths <path/to/graph>  <parent_depth></code>
 *
 * where:
 *
 * <ul>
 * <li><code>max_results_per_cnt</code> is the maximum number of paths to return for any given content</li>
 * <li><code>popularity_threshold</code> is the minimum number of directories to contain the same name for a given content name to be returned</li>
 * <li><code>parent_depth</code> is the depth of parent directories to include in the name (1 = only the file's own name, 2 = the most popular name of its direct parents and itself, ...)
 * </ul>
 *
 * Sample invocation:
 *
 *   $ java -cp ~/swh-environment/swh-graph/java/target/swh-graph-*.jar -Xmx500G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB org.softwareheritage.graph.utils.PopularContentPaths /dev/shm/swh-graph/default/graph 1 \
 *      | pv --line-mode --wait \
 *      | zstdmt \
 *      > /poolswh/softwareheritage/vlorentz/2022-04-25_popular_contents.txt.zst
 */

public class PopularContentPaths {
    private SwhUnidirectionalGraph graph; // Labelled transposed graph
    /*
     * A copy of the graph for each thread to reuse between calls to processChunk
     */
    private ThreadLocal<SwhUnidirectionalGraph> threadGraph;
    private int NUM_THREADS = 96;
    private final int BATCH_SIZE = 10000; /* Number of CSV records to read at once */
    private final long MIN_CONTENT_SIZE = 3; /* Ignore all contents smaller than this */
    private CSVParser csvParser;
    private CSVPrinter csvPrinter;

    final static Logger logger = LoggerFactory.getLogger(PopularContentPaths.class);

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
        if (args.length != 2) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.PopularContentPaths <path/to/graph> <parent_depth>");
            System.exit(1);
        }
        String graphPath = args[0];
        short maxDepth = Short.parseShort(args[1]);

        if (maxDepth < 1) {
            System.err.println("parent_depth must be >= 1");
            System.exit(1);
        }

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));

        PopularContentPaths popular_contents = new PopularContentPaths();
        popular_contents.threadGraph = new ThreadLocal<SwhUnidirectionalGraph>();
        popular_contents.csvParser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);
        popular_contents.csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);

        popular_contents.loadGraph(graphPath);

        popular_contents.run(maxDepth);

        popular_contents.csvPrinter.flush();
        bufferedStdout.flush();
    }

    public void loadGraph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        graph = SwhUnidirectionalGraph.loadLabelledGraphOnly(SwhUnidirectionalGraph.LoadMethod.MAPPED,
                graphBasename + "-transposed", null, null);
        graph.loadProperties(graphBasename);
        graph.properties.loadLabelNames();
        graph.properties.loadContentLength();
        System.err.println("Graph loaded.");
    }

    public void run(short maxDepth) throws InterruptedException, IOException, ExecutionException {

        long totalNodes = graph.numNodes();

        Iterator<CSVRecord> recordIterator = csvParser.iterator();

        CSVRecord header = recordIterator.next();
        /* TODO: Remove support for 'swhid', it's deprecated. */
        if (!header.get(0).equals("SWHID") && !header.get(0).equals("swhid")) {
            throw new RuntimeException("First column of input is not 'SWHID'");
        }
        boolean withSha1 = header.size() >= 2 && header.get(1).equals("sha1");

        if (withSha1) {
            csvPrinter.printRecord("SWHID", "sha1", "length", "filepath", "occurrences");
        } else {
            csvPrinter.printRecord("SWHID", "length", "filepath", "occurrences");
        }

        ProgressLogger pl = new ProgressLogger(logger);
        pl.itemsName = "nodes";
        pl.start("Listing contents...");

        ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);
        Vector<Future> futures = new Vector<Future>();
        for (long i = 0; i < NUM_THREADS; ++i) {
            futures.add(service.submit(() -> {
                try {
                    process(recordIterator, maxDepth, withSha1, pl);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));
        }

        service.shutdown();
        service.awaitTermination(365, TimeUnit.DAYS);

        // Error if any exception occurred
        for (Future future : futures) {
            future.get();
        }

        pl.done();

    }

    /*
     * Worker thread, which reads the <code>recordIterator</code> in chunks in a synchronized block and
     * calls <code>processBatch</code> for each of them.
     */
    private void process(Iterator<CSVRecord> recordIterator, short maxDepth, boolean withSha1, ProgressLogger pl)
            throws IOException {
        SwhUnidirectionalGraph graph = this.graph.copy();

        CSVRecord[] records = new CSVRecord[BATCH_SIZE];

        while (true) {
            // Keep the critical section minimal: only read CSV records to the buffer
            synchronized (csvParser) {
                if (!recordIterator.hasNext()) {
                    break;
                }
                for (int i = 0; i < BATCH_SIZE; i++) {
                    try {
                        records[i] = recordIterator.next();
                    } catch (NoSuchElementException e) {
                        // Reached end of file
                        records[i] = null;
                    }
                }
            }

            // Do the actual work outside the critical section
            processBatch(graph, records, maxDepth, withSha1, pl);
        }
    }

    /*
     * Given an array of CSV records, computes the most popular path of each record and prints it to the
     * shared <code>csvPrinter</code>.
     *
     * <code>graph</code> should be a thread-local copy of the transposed graph at
     * <code>this.graph</code>
     */
    private void processBatch(SwhUnidirectionalGraph graph, CSVRecord[] records, short maxDepth, boolean withSha1,
            ProgressLogger pl) throws IOException {

        long totalNodes = graph.numNodes();
        HashMap<FilepathIds, Long> paths = new HashMap<>();

        for (CSVRecord record : records) {
            if (record == null) {
                // Reached end of file
                break;
            }
            String nodeSWHID;
            String sha1 = null;
            try {
                nodeSWHID = record.get(0);
                if (withSha1) {
                    sha1 = record.get(1);
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                System.err.format("Couldn't parse: %s\n", record);
                throw e;
            }
            long cntNode = graph.getNodeId(nodeSWHID);

            paths.clear();

            Long contentLength = graph.properties.getContentLength(cntNode);
            if (contentLength == null) {
                contentLength = -1L;
            } else if (contentLength >= MIN_CONTENT_SIZE) {
                pushNames(paths, graph, new FilepathIds(maxDepth), cntNode, maxDepth);
            }

            if (paths.size() == 0) {
                if (withSha1) {
                    csvPrinter.printRecord(graph.getSWHID(cntNode), sha1, contentLength, "", "");
                } else {
                    csvPrinter.printRecord(graph.getSWHID(cntNode), contentLength, "", "");
                }
            } else {
                FilepathIds maxFilepathId = null;
                long maxCount = 0;

                for (Map.Entry<FilepathIds, Long> entry : paths.entrySet()) {
                    Long count = entry.getValue();
                    if (count > maxCount) {
                        maxFilepathId = entry.getKey();
                        maxCount = count;
                    }
                }

                String filepath = getFilepathFromParts(maxFilepathId);
                if (filepath == null) {
                    continue;
                }
                if (withSha1) {
                    csvPrinter.printRecord(graph.getSWHID(cntNode), sha1, contentLength, filepath, maxCount);
                } else {
                    csvPrinter.printRecord(graph.getSWHID(cntNode), contentLength, filepath, maxCount);
                }
            }
        }

        synchronized (pl) {
            pl.update(records.length);
        }
    }

    /*
     * For every ancestor path of the given child, appends <code>prefix + parentName[maxDepth-1] + "/" +
     * ... + "/" + parentName[0]</code> where parentName[0] is the id of name of the entry pointing
     * directly to the child.
     *
     * <code>graph</code> should be a thread-local copy of the transposed graph at
     * <code>this.graph</code>
     */
    private void pushNames(HashMap<FilepathIds, Long> names, SwhUnidirectionalGraph graph, FilepathIds prefix,
            long childNodeId, int maxDepth) {
        // 'graph' is the transposed graph, so this is a backward edge (cnt->dir or dir->*)
        ArcLabelledNodeIterator.LabelledArcIterator s = graph.labelledSuccessors(childNodeId);
        boolean pushedAny = false;

        if (maxDepth > 0) {
            long parentNode;
            while ((parentNode = s.nextLong()) >= 0) {
                if (graph.getNodeType(parentNode) != SwhType.DIR) {
                    continue;
                }
                DirEntry[] labels = (DirEntry[]) s.label().get();
                for (DirEntry label : labels) {
                    pushedAny = true;
                    FilepathIds path = new FilepathIds(prefix);
                    path.add(label.filenameId);

                    /* If we can recurse further, also add paths containing the parent */
                    pushNames(names, graph, path, parentNode, maxDepth - 1);
                }
            }
        }

        if (!pushedAny) {
            /*
             * If no parents were pushed, consider the current parent to be a root, and use this partial path
             */
            names.put(prefix, names.getOrDefault(prefix, 0L) + 1);
        }

    }

    /*
     * Given a tuple of filename ids, returns the corresponding file path if possible, or null in case
     * of error fetching any of the parts.
     */
    private String getFilepathFromParts(FilepathIds parts) {
        long[] pathPartIds = parts.getNameIds();
        String filepath = getFilename(pathPartIds[0]);
        if (filepath == null) {
            return null;
        }
        for (short i = 1; i < pathPartIds.length && pathPartIds[i] >= 0; i++) {
            String newFilename = getFilename(pathPartIds[i]);
            if (newFilename == null) {
                return null;
            }
            filepath = newFilename + "/" + filepath; /* Prepend */
        }

        return filepath;
    }

    /*
     * Given a filename id, returns the corresponding file name if possible, or null in case of error.
     */
    private String getFilename(long filenameId) {
        try {
            return new String(graph.properties.getLabelName(filenameId));
        } catch (IllegalArgumentException e) {
            /*
             * https://gitlab.softwareheritage.org/swh/devel/swh-graph/-/issues/4759
             *
             * Caused by: java.lang.IllegalArgumentException: Input byte array has incorrect ending byte at 36
             * at java.base/java.util.Base64$Decoder.decode0(Base64.java:875) at
             * java.base/java.util.Base64$Decoder.decode(Base64.java:566) at
             * org.softwareheritage.graph.SwhGraphProperties.getLabelName(SwhGraphProperties.java:333) at
             * org.softwareheritage.graph.utils.PopularContents.lambda$run$0(PopularContents.java:103)
             */

            System.err.printf("Failed to read filepath %d: %s\n", filenameId, e.toString());
            return null;
        }
    }

    /* tuple of longs (without boxing) */
    class FilepathIds {
        private final short maxSize;
        private final long[] nameIds;

        public FilepathIds(short maxSize) {
            this.maxSize = maxSize;
            this.nameIds = new long[maxSize];
            for (int i = 0; i < maxSize; i++) {
                this.nameIds[i] = -1;
            }
        }

        public FilepathIds(FilepathIds old) {
            this.maxSize = old.maxSize;
            this.nameIds = new long[maxSize];
            for (int i = 0; i < maxSize; i++) {
                this.nameIds[i] = old.nameIds[i];
            }
        }

        public long[] getNameIds() {
            return this.nameIds.clone();
        }

        public void add(long filenameId) {
            for (int i = 0; i < maxSize; i++) {
                if (this.nameIds[i] == -1) {
                    this.nameIds[i] = filenameId;
                    return;
                }
            }
            throw new RuntimeException("Exceeded maxSize");
        }

        @Override
        public int hashCode() {
            long hashCode = 0;
            for (int i = 0; i < maxSize; i++) {
                hashCode ^= Long.hashCode(this.nameIds[i]);
            }
            return (int) (hashCode ^ (hashCode >> 32));
        }

        @Override
        public boolean equals(Object other) {
            long[] otherNameIds = ((FilepathIds) other).nameIds;
            for (int i = 0; i < maxSize; i++) {
                if (this.nameIds[i] != otherNameIds[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    /* Comparator which uses a HashMap indirection. */
    private class SortByHashmap implements Comparator<FilepathIds> {
        private HashMap<FilepathIds, Long> map;
        public SortByHashmap(HashMap<FilepathIds, Long> map) {
            this.map = map;
        }

        public int compare(FilepathIds l1, FilepathIds l2) {
            return map.get(l1).compareTo(map.get(l2));
        }
    }
}

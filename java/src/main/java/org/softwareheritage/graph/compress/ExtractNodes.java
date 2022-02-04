package org.softwareheritage.graph.compress;

import com.github.luben.zstd.ZstdOutputStream;
import com.martiansoftware.jsap.*;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.utils.Sort;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Read a graph dataset and extract all the unique node SWHIDs it contains, including the ones that
 * are not stored as actual objects in the graph, but only <em>referred to</em> by the edges.
 * Additionally, extract the set of all unique edge labels in the graph.
 *
 * <ul>
 * <li>The set of nodes is written in <code>${outputBasename}.nodes.csv.zst</code>, as a
 * zst-compressed sorted list of SWHIDs, one per line.</li>
 * <li>The set of edge labels is written in <code>${outputBasename}.labels.csv.zst</code>, as a
 * zst-compressed sorted list of labels encoded in base64, one per line.</li>
 * <li>The number of unique nodes referred to in the graph is written in a text file,
 * <code>${outputBasename}.nodes.count.txt</code></li>
 * <li>The number of unique edges referred to in the graph is written in a text file,
 * <code>${outputBasename}.edges.count.txt</code></li>
 * <li>The number of unique edge labels is written in a text file,
 * <code>${outputBasename}.labels.count.txt</code></li>
 * <li>Statistics on the number of nodes of each type are written in a text file,
 * <code>${outputBasename}.nodes.stats.txt</code></li>
 * <li>Statistics on the number of edges of each type are written in a text file,
 * <code>${outputBasename}.edges.stats.txt</code></li>
 * </ul>
 *
 * <p>
 * <strong>Rationale:</strong> Because the graph can contain holes, loose objects and dangling
 * objects, some nodes that are referred to as destinations in the edge relationships might not
 * actually be stored in the graph itself. However, to compress the graph using a graph compression
 * library, it is necessary to have a list of <em>all</em> the nodes in the graph, including the
 * ones that are simply referred to by the edges but not actually stored as concrete objects.
 * </p>
 *
 * <p>
 * This class reads the entire graph dataset, and uses <code>sort -u</code> to extract the set of
 * all the unique nodes and unique labels that will be needed as an input for the compression
 * process.
 * </p>
 */
public class ExtractNodes {
    private static JSAPResult parseArgs(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ComposePermutations.class.getName(), "", new Parameter[]{
                    new UnflaggedOption("dataset", JSAP.STRING_PARSER, JSAP.REQUIRED, "Path to the edges dataset"),
                    new UnflaggedOption("outputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED,
                            "Basename of the output files"),

                    new FlaggedOption("format", JSAP.STRING_PARSER, "orc", JSAP.NOT_REQUIRED, 'f', "format",
                            "Format of the input dataset (orc, csv)"),
                    new FlaggedOption("sortBufferSize", JSAP.STRING_PARSER, "30%", JSAP.NOT_REQUIRED, 'S',
                            "sort-buffer-size", "Size of the memory buffer used by sort"),
                    new FlaggedOption("sortTmpDir", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'T', "temp-dir",
                            "Path to the temporary directory used by sort")});

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

    public static void main(String[] args) throws IOException, InterruptedException {
        JSAPResult parsedArgs = parseArgs(args);
        String datasetPath = parsedArgs.getString("dataset");
        String outputBasename = parsedArgs.getString("outputBasename");

        String datasetFormat = parsedArgs.getString("format");
        String sortBufferSize = parsedArgs.getString("sortBufferSize");
        String sortTmpDir = parsedArgs.getString("sortTmpDir", null);

        (new File(sortTmpDir)).mkdirs();

        // Open edge dataset
        GraphDataset dataset;
        if (datasetFormat.equals("orc")) {
            dataset = new ORCGraphDataset(datasetPath);
        } else if (datasetFormat.equals("csv")) {
            dataset = new CSVEdgeDataset(datasetPath);
        } else {
            throw new IllegalArgumentException("Unknown dataset format: " + datasetFormat);
        }

        extractNodes(dataset, outputBasename, sortBufferSize, sortTmpDir);
    }

    public static void extractNodes(GraphDataset dataset, String outputBasename, String sortBufferSize,
            String sortTmpDir) throws IOException, InterruptedException {
        // Spawn node sorting process
        Process nodeSort = Sort.spawnSort(sortBufferSize, sortTmpDir);
        BufferedOutputStream nodeSortStdin = new BufferedOutputStream(nodeSort.getOutputStream());
        BufferedInputStream nodeSortStdout = new BufferedInputStream(nodeSort.getInputStream());
        OutputStream nodesFileOutputStream = new ZstdOutputStream(
                new BufferedOutputStream(new FileOutputStream(outputBasename + ".nodes.csv.zst")));
        NodesOutputThread nodesOutputThread = new NodesOutputThread(nodeSortStdout, nodesFileOutputStream);
        nodesOutputThread.start();

        // Spawn label sorting process
        Process labelSort = Sort.spawnSort(sortBufferSize, sortTmpDir);
        BufferedOutputStream labelSortStdin = new BufferedOutputStream(labelSort.getOutputStream());
        BufferedInputStream labelSortStdout = new BufferedInputStream(labelSort.getInputStream());
        OutputStream labelsFileOutputStream = new ZstdOutputStream(
                new BufferedOutputStream(new FileOutputStream(outputBasename + ".labels.csv.zst")));
        LabelsOutputThread labelsOutputThread = new LabelsOutputThread(labelSortStdout, labelsFileOutputStream);
        labelsOutputThread.start();

        // Read the dataset and write the nodes and labels to the sorting processes
        long[] edgeCount = {0};
        long[][] edgeCountByType = new long[Node.Type.values().length][Node.Type.values().length];
        dataset.readEdges((node) -> {
            nodeSortStdin.write(node);
            nodeSortStdin.write('\n');
        }, (src, dst, label, perm) -> {
            nodeSortStdin.write(src);
            nodeSortStdin.write('\n');
            nodeSortStdin.write(dst);
            nodeSortStdin.write('\n');
            if (label != null) {
                labelSortStdin.write(label);
                labelSortStdin.write('\n');
            }
            edgeCount[0]++;
            // Extract type of src and dst from their SWHID: swh:1:XXX
            byte[] srcTypeBytes = Arrays.copyOfRange(src, 6, 6 + 3);
            byte[] dstTypeBytes = Arrays.copyOfRange(dst, 6, 6 + 3);
            int srcType = Node.Type.byteNameToInt(srcTypeBytes);
            int dstType = Node.Type.byteNameToInt(dstTypeBytes);
            if (srcType != -1 && dstType != -1) {
                edgeCountByType[srcType][dstType]++;
            } else {
                System.err
                        .println("Invalid edge type: " + new String(srcTypeBytes) + " -> " + new String(dstTypeBytes));
                System.exit(1);
            }
        });

        // Wait for sorting processes to finish
        nodeSortStdin.close();
        nodeSort.waitFor();
        labelSortStdin.close();
        labelSort.waitFor();

        nodesOutputThread.join();
        labelsOutputThread.join();

        // Write node, edge and label counts/statistics
        printEdgeCounts(outputBasename, edgeCount[0], edgeCountByType);
        printNodeCounts(outputBasename, nodesOutputThread.getNodeCount(), nodesOutputThread.getNodeTypeCounts());
        printLabelCounts(outputBasename, labelsOutputThread.getLabelCount());
    }

    private static void printEdgeCounts(String basename, long edgeCount, long[][] edgeTypeCounts) throws IOException {
        PrintWriter nodeCountWriter = new PrintWriter(basename + ".edges.count.txt");
        nodeCountWriter.println(edgeCount);
        nodeCountWriter.close();

        PrintWriter nodeTypesCountWriter = new PrintWriter(basename + ".edges.stats.txt");
        TreeMap<String, Long> edgeTypeCountsMap = new TreeMap<>();
        for (Node.Type src : Node.Type.values()) {
            for (Node.Type dst : Node.Type.values()) {
                long cnt = edgeTypeCounts[Node.Type.toInt(src)][Node.Type.toInt(dst)];
                if (cnt > 0)
                    edgeTypeCountsMap.put(src.toString().toLowerCase() + ":" + dst.toString().toLowerCase(), cnt);
            }
        }
        for (Map.Entry<String, Long> entry : edgeTypeCountsMap.entrySet()) {
            nodeTypesCountWriter.println(entry.getKey() + " " + entry.getValue());
        }
        nodeTypesCountWriter.close();
    }

    private static void printNodeCounts(String basename, long nodeCount, long[] nodeTypeCounts) throws IOException {
        PrintWriter nodeCountWriter = new PrintWriter(basename + ".nodes.count.txt");
        nodeCountWriter.println(nodeCount);
        nodeCountWriter.close();

        PrintWriter nodeTypesCountWriter = new PrintWriter(basename + ".nodes.stats.txt");
        TreeMap<String, Long> nodeTypeCountsMap = new TreeMap<>();
        for (Node.Type v : Node.Type.values()) {
            nodeTypeCountsMap.put(v.toString().toLowerCase(), nodeTypeCounts[Node.Type.toInt(v)]);
        }
        for (Map.Entry<String, Long> entry : nodeTypeCountsMap.entrySet()) {
            nodeTypesCountWriter.println(entry.getKey() + " " + entry.getValue());
        }
        nodeTypesCountWriter.close();
    }

    private static void printLabelCounts(String basename, long labelCount) throws IOException {
        PrintWriter nodeCountWriter = new PrintWriter(basename + ".labels.count.txt");
        nodeCountWriter.println(labelCount);
        nodeCountWriter.close();
    }

    private static class NodesOutputThread extends Thread {
        private final InputStream sortedNodesStream;
        private final OutputStream nodesOutputStream;

        private long nodeCount = 0;
        private final long[] nodeTypeCounts = new long[Node.Type.values().length];

        NodesOutputThread(InputStream sortedNodesStream, OutputStream nodesOutputStream) {
            this.sortedNodesStream = sortedNodesStream;
            this.nodesOutputStream = nodesOutputStream;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(sortedNodesStream, StandardCharsets.UTF_8));
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    nodesOutputStream.write(line.getBytes(StandardCharsets.UTF_8));
                    nodesOutputStream.write('\n');
                    nodeCount++;
                    try {
                        Node.Type nodeType = Node.Type.fromStr(line.split(":")[2]);
                        nodeTypeCounts[Node.Type.toInt(nodeType)]++;
                    } catch (ArrayIndexOutOfBoundsException e) {
                        System.err.println("Error parsing SWHID: " + line);
                        System.exit(1);
                    }
                }
                nodesOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public long getNodeCount() {
            return nodeCount;
        }

        public long[] getNodeTypeCounts() {
            return nodeTypeCounts;
        }
    }

    private static class LabelsOutputThread extends Thread {
        private final InputStream sortedLabelsStream;
        private final OutputStream labelsOutputStream;

        private long labelCount = 0;

        LabelsOutputThread(InputStream sortedLabelsStream, OutputStream labelsOutputStream) {
            this.labelsOutputStream = labelsOutputStream;
            this.sortedLabelsStream = sortedLabelsStream;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(sortedLabelsStream, StandardCharsets.UTF_8));
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    labelsOutputStream.write(line.getBytes(StandardCharsets.UTF_8));
                    labelsOutputStream.write('\n');
                    labelCount++;
                }
                labelsOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public long getLabelCount() {
            return labelCount;
        }
    }
}

package org.softwareheritage.graph.maps;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.labels.DirEntry;
import org.softwareheritage.graph.labels.SwhLabel;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class LabelMapBuilder {

    final static String SORT_BUFFER_SIZE = "40%";

    final static Logger logger = LoggerFactory.getLogger(LabelMapBuilder.class);

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(LabelMapBuilder.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),
                            new FlaggedOption("debugPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd',
                                    "debug-path", "Store the intermediate representation here for debug"),

                            new FlaggedOption("tmpDir", JSAP.STRING_PARSER, "tmp", JSAP.NOT_REQUIRED, 't', "tmp",
                                    "Temporary directory path"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    public static void main(String[] args) throws IOException {
        JSAPResult config = parse_args(args);
        String graphPath = config.getString("graphPath");
        String tmpDir = config.getString("tmpDir");
        String debugPath = config.getString("debugPath");

        logger.info("Starting label map generation...");
        computeLabelMap(graphPath, debugPath, tmpDir);
        logger.info("Label map generation ended.");
    }

    @SuppressWarnings("unchecked") // Suppress warning for Object2LongFunction cast
    static Object2LongFunction<String> loadMPH(String mphBasename) throws IOException {
        Object2LongFunction<String> mphMap = null;
        try {
            logger.info("loading MPH function...");
            mphMap = (Object2LongFunction<String>) BinIO.loadObject(mphBasename + ".mph");
            logger.info("MPH function loaded");
        } catch (ClassNotFoundException e) {
            logger.error("unknown class object in .mph file: " + e);
            System.exit(2);
        }
        return mphMap;
    }

    static long getMPHSize(Object2LongFunction<String> mph) {
        return (mph instanceof Size64) ? ((Size64) mph).size64() : mph.size();
    }

    static void computeLabelMap(String graphPath, String debugPath, String tmpDir) throws IOException {
        // Compute intermediate representation as: "<src node id> <dst node id> <label ids>\n"
        ImmutableGraph graph = BVGraph.loadMapped(graphPath);

        Object2LongFunction<String> swhIdMph = loadMPH(graphPath);
        long[][] orderMap = LongBigArrays.newBigArray(getMPHSize(swhIdMph));
        BinIO.loadLongs(graphPath + ".order", orderMap);

        Object2LongFunction<String> filenameMph = loadMPH(graphPath + "-labels");
        long numFilenames = getMPHSize(filenameMph);

        int totalLabelWidth = DirEntry.labelWidth(numFilenames);

        ProgressLogger plInter = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plInter.itemsName = "edges";
        plInter.expectedUpdates = graph.numArcs();

        /*
         * Pass the intermediate representation to sort(1) so that we see the labels in the order they will
         * appear in the label file.
         */
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("sort", "-k1,1n", "-k2,2n", // Numerical sort
                "--numeric-sort", "--buffer-size", SORT_BUFFER_SIZE, "--temporary-directory", tmpDir);
        Process sort = processBuilder.start();
        BufferedOutputStream sort_stdin = new BufferedOutputStream(sort.getOutputStream());
        BufferedInputStream sort_stdout = new BufferedInputStream(sort.getInputStream());

        plInter.start("Piping intermediate representation to sort(1)");

        final FastBufferedInputStream fbis = new FastBufferedInputStream(System.in);
        var charset = StandardCharsets.US_ASCII;
        byte[] array = new byte[1024];
        for (long line = 0;; line++) {
            int start = 0, len;
            while ((len = fbis.readLine(array, start, array.length - start,
                    FastBufferedInputStream.ALL_TERMINATORS)) == array.length - start) {
                start += len;
                array = ByteArrays.grow(array, array.length + 1);
            }
            if (len == -1)
                break; // EOF
            final int lineLength = start + len;

            // Skip whitespace at the start of the line.
            int offset = 0;
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;
            if (offset == lineLength) {
                continue;
            }
            if (array[0] == '#')
                continue;

            // Scan source id.
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            final String ss = new String(array, start, offset - start, charset);

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;
            if (offset == lineLength) {
                logger.error("Error at line " + line + ": no target");
                continue;
            }

            // Scan target ID
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            final String ts = new String(array, start, offset - start, charset);

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;
            if (offset == lineLength)
                continue; // No label, skip

            // Scan label
            start = offset;
            while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                offset++;
            final String ls = new String(array, start, offset - start, charset);

            // Skip whitespace between identifiers.
            while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ')
                offset++;
            // Scan permission
            int permission = 0;
            if (offset < lineLength) {
                start = offset;
                while (offset < lineLength && (array[offset] < 0 || array[offset] > ' '))
                    offset++;
                permission = Integer.parseInt(new String(array, start, offset - start, charset));
            }

            // System.err.format("DEBUG: read %s %s %s %d\n", ss, ts, ls, permission);

            long s = swhIdMph.getLong(ss);
            long t = swhIdMph.getLong(ts);
            long filenameId = filenameMph.getLong(ls);

            long srcNode = BigArrays.get(orderMap, s);
            long dstNode = BigArrays.get(orderMap, t);

            sort_stdin.write((srcNode + "\t" + dstNode + "\t" + filenameId + "\t" + permission + "\n")
                    .getBytes(StandardCharsets.US_ASCII));
            plInter.lightUpdate();
        }
        plInter.done();
        sort_stdin.close();

        // Get the sorted output and write the labels and label offsets
        ProgressLogger plLabels = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plLabels.itemsName = "nodes";
        plLabels.expectedUpdates = graph.numNodes();

        FileWriter debugFile = null;
        if (debugPath != null) {
            debugFile = new FileWriter(debugPath);
        }

        OutputBitStream labels = new OutputBitStream(
                new File(graphPath + "-labelled" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION));
        OutputBitStream offsets = new OutputBitStream(
                new File(graphPath + "-labelled" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION));
        offsets.writeGamma(0);

        Scanner sortOutput = new Scanner(sort_stdout, StandardCharsets.US_ASCII);

        NodeIterator it = graph.nodeIterator();
        long labelSrcNode = -1;
        long labelDstNode = -1;
        long labelFilenameId = -1;
        int labelPermission = -1;

        while (it.hasNext()) {
            long srcNode = it.nextLong();

            // Fill a hashmap with the labels of each edge starting from this node
            HashMap<Long, List<DirEntry>> successorsLabels = new HashMap<>();
            while (labelSrcNode <= srcNode) {
                if (labelSrcNode == srcNode) {
                    successorsLabels.computeIfAbsent(labelDstNode, k -> new ArrayList<>())
                            .add(new DirEntry(labelFilenameId, labelPermission));
                    if (debugFile != null) {
                        debugFile.write(labelSrcNode + " " + labelDstNode + " " + labelFilenameId + " "
                                + labelPermission + "\n");
                    }
                }

                if (!sortOutput.hasNext())
                    break;

                String line = sortOutput.nextLine();
                String[] parts = line.split("\\t");
                labelSrcNode = Long.parseLong(parts[0]);
                labelDstNode = Long.parseLong(parts[1]);
                labelFilenameId = Long.parseLong(parts[2]);
                labelPermission = Integer.parseInt(parts[3]);
            }

            int bits = 0;
            LazyLongIterator s = it.successors();
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                List<DirEntry> currentLabels = successorsLabels.getOrDefault(dstNode, Collections.emptyList());
                SwhLabel l = new SwhLabel("edgelabel", totalLabelWidth, currentLabels.toArray(new DirEntry[0]));
                bits += l.toBitStream(labels, -1);
            }
            offsets.writeGamma(bits);

            plLabels.lightUpdate();
        }
        labels.close();
        offsets.close();
        plLabels.done();
        if (debugFile != null) {
            debugFile.close();
        }

        PrintWriter pw = new PrintWriter(new FileWriter((new File(graphPath)).getName() + "-labelled.properties"));
        pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
        pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + SwhLabel.class.getName()
                + "(DirEntry," + totalLabelWidth + ")");
        pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = " + graphPath);
        pw.close();
    }
}

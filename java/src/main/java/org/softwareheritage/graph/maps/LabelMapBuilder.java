package org.softwareheritage.graph.maps;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.FixedWidthIntLabel;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.SwhPID;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class LabelMapBuilder {

    final static String SORT_BUFFER_SIZE = "40%";

    final static Logger logger = LoggerFactory.getLogger(LabelMapBuilder.class);

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(
                    LabelMapBuilder.class.getName(),
                    "",
                    new Parameter[] {
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                    'g', "graph", "Basename of the compressed graph"),
                            new FlaggedOption("debugPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED,
                                    'd', "debug-path", "Store the intermediate representation here for debug"),

                            new FlaggedOption("tmpDir", JSAP.STRING_PARSER, "tmp", JSAP.NOT_REQUIRED,
                                    't', "tmp", "Temporary directory path"),
                    }
            );

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

    static long getMPHSize(Object2LongFunction<String> mph)
    {
        return (mph instanceof Size64) ? ((Size64) mph).size64() : mph.size();
    }

    static void computeLabelMap(String graphPath, String debugPath, String tmpDir)
        throws IOException
    {
        // Compute intermediate representation in the format "<src node id> <dst node id> <label ids>\n"
        ImmutableGraph graph = BVGraph.loadMapped(graphPath);
        NodeIdMap nodeMap = new NodeIdMap(graphPath, graph.numNodes());

        Object2LongFunction<String> labelMPH = loadMPH(graphPath + "-labels");
        long numLabels = getMPHSize(labelMPH);
        int labelWidth = (int) Math.ceil(Math.log(numLabels) / Math.log(2));
        if (labelWidth > 30) {
            logger.error("FIXME: Too many labels, we can't handle more than 2^30 for now.");
            System.exit(2);
        }

        ProgressLogger plInter = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        plInter.itemsName = "edges";
        plInter.expectedUpdates = graph.numArcs();

        // Pass the intermediate representation to sort(1) so that we see the labels in the
        // order they will appear in the label file.
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(
                "sort",
                "-k1,1n", "-k2,2n", "-k3,3n",  // Numerical sort on all fields
                "--numeric-sort",
                "--buffer-size", SORT_BUFFER_SIZE,
                "--temporary-directory", tmpDir
        );
        Process sort = processBuilder.start();
        BufferedOutputStream sort_stdin = new BufferedOutputStream(sort.getOutputStream());
        BufferedInputStream sort_stdout = new BufferedInputStream(sort.getInputStream());

        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(System.in,
                StandardCharsets.US_ASCII));
        LineIterator edgeIterator = new LineIterator(buffer);

        plInter.start("Piping intermediate representation to sort(1)");
        int i = 0;
        while (edgeIterator.hasNext()) {
            // if (i > 100000) break;
            String[] edge = edgeIterator.next().toString().split(" ");
            if (edge.length < 3)
                continue;

            SwhPID srcPid = new SwhPID(edge[0]);
            SwhPID dstPid = new SwhPID(edge[1]);
            String label = edge[2];

            // FIXME: Very slow. Use MPH + .order instead?
            long srcNode = nodeMap.getNodeId(srcPid);
            long dstNode = nodeMap.getNodeId(dstPid);
            long labelId = labelMPH.getLong(label);

            sort_stdin.write((srcNode + "\t" + dstNode + "\t" + labelId + "\n")
                    .getBytes(StandardCharsets.US_ASCII));
            plInter.lightUpdate();
            i++;
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

        OutputBitStream labels = new OutputBitStream(new File(graphPath + "-labelled" + BitStreamArcLabelledImmutableGraph.LABELS_EXTENSION));
        OutputBitStream offsets = new OutputBitStream(new File(graphPath + "-labelled" + BitStreamArcLabelledImmutableGraph.LABEL_OFFSETS_EXTENSION));
        offsets.writeGamma(0);

        Scanner sortOutput = new Scanner(sort_stdout, StandardCharsets.US_ASCII);

        NodeIterator it = graph.nodeIterator();
        long labelSrcNode = -1;
        long labelDstNode = -1;
        long labelId = -1;

        while (it.hasNext()) {
            long srcNode = it.nextLong();

            // Fill a hashmap with the labels of each edge starting from this node
            HashMap<Long, Long> successorsLabels = new HashMap<>();
            while (labelSrcNode <= srcNode && sortOutput.hasNext()) {
                if (labelSrcNode == srcNode) {
                    // FIXME: This overrides the previous label in case of multiple src<->dst edges
                    //        Changing this requires using a FixedWidthIntList.
                    successorsLabels.put(labelDstNode, labelId);
                    if (debugFile != null) {
                        debugFile.write(labelSrcNode + " " + labelDstNode + " " + labelId);
                    }
                }

                String line = sortOutput.nextLine();
                String[] parts = line.split("\\t");
                labelSrcNode = Long.parseLong(parts[0]);
                labelDstNode = Long.parseLong(parts[1]);
                labelId = Long.parseLong(parts[2]);
            }

            int bits = 0;
            LazyLongIterator s = it.successors();
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                bits += labels.writeLong(successorsLabels.getOrDefault(dstNode, -1L), labelWidth);
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

        PrintWriter pw = new PrintWriter(new FileWriter( graphPath + "-labelled.properties" ));
        pw.println(ImmutableGraph.GRAPHCLASS_PROPERTY_KEY + " = " + BitStreamArcLabelledImmutableGraph.class.getName());
        pw.println(BitStreamArcLabelledImmutableGraph.LABELSPEC_PROPERTY_KEY + " = " + FixedWidthIntLabel.class.getName() + "(TEST," + labelWidth + ")" );
        pw.println(ArcLabelledImmutableGraph.UNDERLYINGGRAPH_PROPERTY_KEY + " = " + graphPath);
        pw.close();
    }
}

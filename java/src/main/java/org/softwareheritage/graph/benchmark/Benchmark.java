package org.softwareheritage.graph.benchmark;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.StringJoiner;
import java.util.function.Function;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.benchmark.utils.Random;
import org.softwareheritage.graph.benchmark.utils.Statistics;

/**
 * Benchmark common utility functions.
 *
 * @author The Software Heritage developers
 */

public class Benchmark {
    /**
     * Input arguments.
     */
    public class Args {
        /** Basename of the compressed graph */
        public String graphPath;
        /** Number of random nodes to use for the benchmark */
        public int nbNodes;
        /** File name for CSV format benchmark log */
        public String logFile;
        /** Random generator */
        public Random random;
    }

    /** Command line arguments */
    public Args args;
    /** CSV separator for log file */
    final String CSV_SEPARATOR = ";";

    /**
     * Constructor.
     */
    public Benchmark() {
        this.args = new Args();
    }

    /**
     * Parses benchmark command line arguments.
     *
     * @param args command line arguments
     */
    public void parseCommandLineArgs(String[] args) throws JSAPException {
        SimpleJSAP jsap = new SimpleJSAP(Benchmark.class.getName(),
                                         "Benchmark tool for Software Heritage use-cases scenarios.",
                                         new Parameter[] {
                                             new UnflaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                                     JSAP.NOT_GREEDY, "The basename of the compressed graph."),
                                             new FlaggedOption("nbNodes", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'n',
                                                     "nb-nodes", "Number of random nodes used to do the benchmark."),
                                             new FlaggedOption("logFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'l',
                                                     "log-file", "File name to output CSV format benchmark log."),
                                             new FlaggedOption("seed", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's',
                                                     "seed", "Random generator seed."),
                                         });

        JSAPResult config = jsap.parse(args);
        if (jsap.messagePrinted()) {
            System.exit(1);
        }

        this.args.graphPath = config.getString("graphPath");
        this.args.nbNodes = config.getInt("nbNodes");
        this.args.logFile = config.getString("logFile");
        this.args.random = config.contains("seed") ? new Random(config.getLong("seed")) : new Random();
    }

    /**
     * Creates CSV file for log output.
     */
    public void createCSVLogFile() throws IOException {
        try (Writer csvLog = new BufferedWriter(new FileWriter(args.logFile))) {
            StringJoiner csvHeader = new StringJoiner(CSV_SEPARATOR);
            csvHeader.add("use case name")
            .add("SWH PID")
            .add("number of edges accessed")
            .add("traversal timing")
            .add("pid2node timing")
            .add("node2pid timing");
            csvLog.write(csvHeader.toString() + "\n");
        }
    }

    /**
     * Times a specific endpoint and outputs individual datapoints along with aggregated statistics.
     *
     * @param useCaseName benchmark use-case name
     * @param graph compressed graph used in the benchmark
     * @param nodeIds node ids to use as starting point for the endpoint traversal
     * @param operation endpoint function to benchmark
     * @param dstFmt destination formatted string as described in the <a
     * href="https://docs.softwareheritage.org/devel/swh-graph/api.html#walk">API</a>
     * @param algorithm traversal algorithm used in endpoint call (either "dfs" or "bfs")
     */
    public void timeEndpoint(String useCaseName, Graph graph, long[] nodeIds,
                             Function<Endpoint.Input, Endpoint.Output> operation, String dstFmt, String algorithm)
    throws IOException {
        ArrayList<Double> timings = new ArrayList<>();
        ArrayList<Double> timingsNormalized = new ArrayList<>();
        ArrayList<Double> nbEdgesAccessed = new ArrayList<>();

        final boolean append = true;
        try (Writer csvLog = new BufferedWriter(new FileWriter(args.logFile, append))) {
            for (long nodeId : nodeIds) {
                SwhPID swhPID = graph.getSwhPID(nodeId);

                Endpoint.Output output = (dstFmt == null)
                                         ? operation.apply(new Endpoint.Input(swhPID))
                                         : operation.apply(new Endpoint.Input(swhPID, dstFmt, algorithm));

                StringJoiner csvLine = new StringJoiner(CSV_SEPARATOR);
                csvLine.add(useCaseName)
                .add(swhPID.toString())
                .add(Long.toString(output.meta.nbEdgesAccessed))
                .add(Double.toString(output.meta.timings.traversal))
                .add(Double.toString(output.meta.timings.pid2node))
                .add(Double.toString(output.meta.timings.node2pid));
                csvLog.write(csvLine.toString() + "\n");

                timings.add(output.meta.timings.traversal);
                nbEdgesAccessed.add((double) output.meta.nbEdgesAccessed);
                if (output.meta.nbEdgesAccessed != 0) {
                    timingsNormalized.add(output.meta.timings.traversal / output.meta.nbEdgesAccessed);
                }
            }
        }

        System.out.println("\n" + useCaseName + " use-case:");

        System.out.println("timings:");
        Statistics stats = new Statistics(timings);
        stats.printAll();

        System.out.println("timings normalized:");
        Statistics statsNormalized = new Statistics(timingsNormalized);
        statsNormalized.printAll();

        System.out.println("nb edges accessed:");
        Statistics statsNbEdgesAccessed = new Statistics(nbEdgesAccessed);
        statsNbEdgesAccessed.printAll();
    }

    /**
     * Same as {@link timeEndpoint} but without destination or algorithm specified to endpoint call.
     */
    public void timeEndpoint(String useCaseName, Graph graph, long[] nodeIds,
                             Function<Endpoint.Input, Endpoint.Output> operation) throws IOException {
        timeEndpoint(useCaseName, graph, nodeIds, operation, null, null);
    }
}

package org.softwareheritage.graph.experiments.topology;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;

public class InOutDegree {
    private InOutDegree() {
    }

    private static final int NODE_ARRAY_SIZE = Node.Type.values().length + 1;
    private static final int TYPE_ALL = Node.Type.values().length;
    private static final int TYPE_CNT = Node.Type.toInt(Node.Type.CNT);
    private static final int TYPE_DIR = Node.Type.toInt(Node.Type.DIR);
    private static final int TYPE_REV = Node.Type.toInt(Node.Type.REV);
    private static final int TYPE_REL = Node.Type.toInt(Node.Type.REL);
    private static final int TYPE_SNP = Node.Type.toInt(Node.Type.SNP);
    private static final int TYPE_ORI = Node.Type.toInt(Node.Type.ORI);

    public static long[] outdegreeTypes(final Graph graph, long node) {
        long[] out = new long[NODE_ARRAY_SIZE];
        var successors = graph.successors(node);
        long neighbor;
        while ((neighbor = successors.nextLong()) != -1) {
            out[Node.Type.toInt(graph.getNodeType(neighbor))]++;
            out[TYPE_ALL]++;
        }
        return out;
    }

    public static long[] indegreeTypes(final Graph graph, long node) {
        return outdegreeTypes(graph.transpose(), node);
    }

    public static void writeDistribution(HashMap<Long, Long> distribution, String filename) throws IOException {
        PrintWriter f = new PrintWriter(new FileWriter(filename));
        TreeMap<Long, Long> sortedDistribution = new TreeMap<>(distribution);
        for (Map.Entry<Long, Long> entry : sortedDistribution.entrySet()) {
            f.println(entry.getKey() + " " + entry.getValue());
        }
        f.close();
    }

    public static void run(final Graph graph, String resultsDir) throws IOException {
        var cnt_in_dir = new HashMap<Long, Long>();
        var dir_in_dir = new HashMap<Long, Long>();
        var dir_in_rev = new HashMap<Long, Long>();
        var dir_in_all = new HashMap<Long, Long>();
        var dir_out_all = new HashMap<Long, Long>();
        var dir_out_dir = new HashMap<Long, Long>();
        var dir_out_cnt = new HashMap<Long, Long>();
        var dir_out_rev = new HashMap<Long, Long>();
        var rev_in_dir = new HashMap<Long, Long>();
        var rev_in_rel = new HashMap<Long, Long>();
        var rev_in_rev = new HashMap<Long, Long>();
        var rev_in_snp = new HashMap<Long, Long>();
        var rev_in_all = new HashMap<Long, Long>();
        var rev_out_rev = new HashMap<Long, Long>();
        var rel_in_snp = new HashMap<Long, Long>();
        var snp_in_ori = new HashMap<Long, Long>();
        var snp_out_all = new HashMap<Long, Long>();
        var snp_out_rel = new HashMap<Long, Long>();
        var snp_out_rev = new HashMap<Long, Long>();
        var ori_out_snp = new HashMap<Long, Long>();

        final ProgressLogger pl = new ProgressLogger();
        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();
        pl.start("Scanning...");

        long[] in;
        long[] out;

        for (long i = graph.numNodes(); i-- != 0;) {
            switch (graph.getNodeType(i)) {
                case CNT:
                    cnt_in_dir.merge(graph.indegree(i), 1L, Long::sum);
                case DIR:
                    in = indegreeTypes(graph, i);
                    out = outdegreeTypes(graph, i);
                    dir_in_all.merge(in[TYPE_ALL], 1L, Long::sum);
                    dir_out_all.merge(out[TYPE_ALL], 1L, Long::sum);
                    dir_in_dir.merge(in[TYPE_DIR], 1L, Long::sum);
                    dir_in_rev.merge(in[TYPE_REV], 1L, Long::sum);
                    dir_out_cnt.merge(out[TYPE_CNT], 1L, Long::sum);
                    dir_out_dir.merge(out[TYPE_DIR], 1L, Long::sum);
                    dir_out_rev.merge(out[TYPE_REV], 1L, Long::sum);
                case REV:
                    in = indegreeTypes(graph, i);
                    out = outdegreeTypes(graph, i);
                    rev_in_all.merge(in[TYPE_ALL], 1L, Long::sum);
                    rev_in_dir.merge(in[TYPE_DIR], 1L, Long::sum);
                    rev_in_rev.merge(in[TYPE_REV], 1L, Long::sum);
                    rev_in_rel.merge(in[TYPE_REL], 1L, Long::sum);
                    rev_in_snp.merge(in[TYPE_SNP], 1L, Long::sum);
                    rev_out_rev.merge(out[TYPE_REV], 1L, Long::sum);
                case REL:
                    rel_in_snp.merge(graph.indegree(i), 1L, Long::sum);
                case SNP:
                    out = outdegreeTypes(graph, i);
                    snp_in_ori.merge(graph.indegree(i), 1L, Long::sum);
                    snp_out_all.merge(out[TYPE_ALL], 1L, Long::sum);
                    snp_out_rel.merge(out[TYPE_REL], 1L, Long::sum);
                    snp_out_rev.merge(out[TYPE_REV], 1L, Long::sum);
                case ORI:
                    ori_out_snp.merge(graph.outdegree(i), 1L, Long::sum);
            }

            pl.update();
        }

        pl.done();

        writeDistribution(cnt_in_dir, resultsDir + "/cnt_in_dir.txt");
        writeDistribution(dir_in_dir, resultsDir + "/dir_in_dir.txt");
        writeDistribution(dir_in_rev, resultsDir + "/dir_in_rev.txt");
        writeDistribution(dir_in_all, resultsDir + "/dir_in_all.txt");
        writeDistribution(dir_out_all, resultsDir + "/dir_out_all.txt");
        writeDistribution(dir_out_dir, resultsDir + "/dir_out_dir.txt");
        writeDistribution(dir_out_cnt, resultsDir + "/dir_out_cnt.txt");
        writeDistribution(dir_out_rev, resultsDir + "/dir_out_rev.txt");
        writeDistribution(rev_in_dir, resultsDir + "/rev_in_dir.txt");
        writeDistribution(rev_in_rel, resultsDir + "/rev_in_rel.txt");
        writeDistribution(rev_in_rev, resultsDir + "/rev_in_rev.txt");
        writeDistribution(rev_in_snp, resultsDir + "/rev_in_snp.txt");
        writeDistribution(rev_in_all, resultsDir + "/rev_in_all.txt");
        writeDistribution(rev_out_rev, resultsDir + "/rev_out_rev.txt");
        writeDistribution(rel_in_snp, resultsDir + "/rel_in_snp.txt");
        writeDistribution(snp_in_ori, resultsDir + "/snp_in_ori.txt");
        writeDistribution(snp_out_all, resultsDir + "/snp_out_all.txt");
        writeDistribution(snp_out_rel, resultsDir + "/snp_out_rel.txt");
        writeDistribution(snp_out_rev, resultsDir + "/snp_out_rev.txt");
        writeDistribution(ori_out_snp, resultsDir + "/ori_out_snp.txt");
    }

    static public void main(final String[] arg)
            throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException,
            NoSuchMethodException, JSAPException, IOException, ClassNotFoundException {
        final SimpleJSAP jsap = new SimpleJSAP(InOutDegree.class.getName(),
                "Computes in and out degrees of the given SWHGraph",
                new Parameter[]{
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                JSAP.NOT_GREEDY, "The basename of the graph."),
                        new UnflaggedOption("resultsDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED,
                                JSAP.NOT_GREEDY, "The directory of the resulting files."),});

        final JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted())
            System.exit(1);

        final String basename = jsapResult.getString("basename");
        final String resultsDir = jsapResult.userSpecified("resultsDir")
                ? jsapResult.getString("resultsDir")
                : basename;

        final ProgressLogger pl = new ProgressLogger();

        Graph graph = new Graph(basename);
        run(graph, resultsDir);
    }
}

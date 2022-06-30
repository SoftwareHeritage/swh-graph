/*
 * Copyright (c) 2020 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.experiments.topology;

import java.io.File;
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
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.SwhType;

public class InOutDegree {
    private InOutDegree() {
    }

    private static final int NODE_ARRAY_SIZE = SwhType.values().length + 1;
    private static final int TYPE_ALL = SwhType.values().length;
    private static final int TYPE_CNT = SwhType.toInt(SwhType.CNT);
    private static final int TYPE_DIR = SwhType.toInt(SwhType.DIR);
    private static final int TYPE_REV = SwhType.toInt(SwhType.REV);
    private static final int TYPE_REL = SwhType.toInt(SwhType.REL);
    private static final int TYPE_SNP = SwhType.toInt(SwhType.SNP);
    private static final int TYPE_ORI = SwhType.toInt(SwhType.ORI);

    public static long[] outdegreeTypes(final SwhBidirectionalGraph graph, long node) {
        long[] out = new long[NODE_ARRAY_SIZE];
        var successors = graph.successors(node);
        long neighbor;
        while ((neighbor = successors.nextLong()) != -1) {
            out[SwhType.toInt(graph.getNodeType(neighbor))]++;
            out[TYPE_ALL]++;
        }
        return out;
    }

    public static long[] indegreeTypes(final SwhBidirectionalGraph graph, long node) {
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

    public static void run(final SwhBidirectionalGraph graph, String resultsDir) throws IOException {
        // Per-type
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

        // Aggregated per layer
        var full_in = new HashMap<Long, Long>();
        var full_out = new HashMap<Long, Long>();
        var dircnt_in = new HashMap<Long, Long>();
        var dircnt_out = new HashMap<Long, Long>();
        var orisnp_in = new HashMap<Long, Long>();
        var orisnp_out = new HashMap<Long, Long>();
        var relrev_in = new HashMap<Long, Long>();
        var relrev_out = new HashMap<Long, Long>();
        var rev_in = rev_in_rev; // alias for single-type layer
        var rev_out = rev_out_rev;

        final ProgressLogger pl = new ProgressLogger();
        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();
        pl.start("Scanning...");

        long[] in;
        long[] out;

        for (long i = graph.numNodes(); i-- != 0;) {
            long d_in = graph.indegree(i);
            long d_out = graph.outdegree(i);

            full_in.merge(d_in, 1L, Long::sum);
            full_out.merge(d_out, 1L, Long::sum);

            switch (graph.getNodeType(i)) {
                case CNT:
                    cnt_in_dir.merge(d_in, 1L, Long::sum);

                    dircnt_in.merge(d_in, 1L, Long::sum);
                    dircnt_out.merge(0L, 1L, Long::sum);
                    break;
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

                    dircnt_in.merge(in[TYPE_DIR] + in[TYPE_CNT], 1L, Long::sum);
                    dircnt_out.merge(out[TYPE_DIR] + out[TYPE_CNT], 1L, Long::sum);
                    break;
                case REV:
                    in = indegreeTypes(graph, i);
                    out = outdegreeTypes(graph, i);

                    rev_in_all.merge(in[TYPE_ALL], 1L, Long::sum);
                    rev_in_dir.merge(in[TYPE_DIR], 1L, Long::sum);
                    rev_in_rev.merge(in[TYPE_REV], 1L, Long::sum);
                    rev_in_rel.merge(in[TYPE_REL], 1L, Long::sum);
                    rev_in_snp.merge(in[TYPE_SNP], 1L, Long::sum);
                    rev_out_rev.merge(out[TYPE_REV], 1L, Long::sum);

                    relrev_in.merge(in[TYPE_REL] + in[TYPE_REV], 1L, Long::sum);
                    relrev_out.merge(out[TYPE_REL] + out[TYPE_REV], 1L, Long::sum);
                    break;
                case REL:
                    rel_in_snp.merge(d_in, 1L, Long::sum);

                    relrev_in.merge(0L, 1L, Long::sum);
                    relrev_out.merge(d_out, 1L, Long::sum);
                    break;
                case SNP:
                    out = outdegreeTypes(graph, i);

                    snp_in_ori.merge(d_in, 1L, Long::sum);
                    snp_out_all.merge(out[TYPE_ALL], 1L, Long::sum);
                    snp_out_rel.merge(out[TYPE_REL], 1L, Long::sum);
                    snp_out_rev.merge(out[TYPE_REV], 1L, Long::sum);

                    orisnp_in.merge(d_in, 1L, Long::sum);
                    orisnp_out.merge(out[TYPE_REL] + out[TYPE_REV], 1L, Long::sum);
                    break;
                case ORI:
                    ori_out_snp.merge(d_out, 1L, Long::sum);

                    orisnp_in.merge(0L, 1L, Long::sum);
                    orisnp_out.merge(d_out, 1L, Long::sum);
                    break;
                default :
                    pl.logger().warn("Invalid node type at pos {}", i);
                    break;
            }

            pl.update();
        }

        pl.done();

        (new File(resultsDir)).mkdir();
        writeDistribution(full_in, resultsDir + "/full_in.txt");
        writeDistribution(full_out, resultsDir + "/full_out.txt");
        writeDistribution(dircnt_in, resultsDir + "/dir+cnt_in.txt");
        writeDistribution(dircnt_out, resultsDir + "/dir+cnt_out.txt");
        writeDistribution(relrev_in, resultsDir + "/rel+rev_in.txt");
        writeDistribution(relrev_out, resultsDir + "/rel+rev_out.txt");
        writeDistribution(orisnp_in, resultsDir + "/ori+snp_in.txt");
        writeDistribution(orisnp_out, resultsDir + "/ori+snp_out.txt");
        writeDistribution(rev_in, resultsDir + "/rev_in.txt");
        writeDistribution(rev_out, resultsDir + "/rev_out.txt");

        String resultsTypeDir = resultsDir + "/per_type";
        (new File(resultsTypeDir)).mkdir();
        writeDistribution(cnt_in_dir, resultsTypeDir + "/cnt_in_dir.txt");
        writeDistribution(dir_in_dir, resultsTypeDir + "/dir_in_dir.txt");
        writeDistribution(dir_in_rev, resultsTypeDir + "/dir_in_rev.txt");
        writeDistribution(dir_in_all, resultsTypeDir + "/dir_in_all.txt");
        writeDistribution(dir_out_all, resultsTypeDir + "/dir_out_all.txt");
        writeDistribution(dir_out_dir, resultsTypeDir + "/dir_out_dir.txt");
        writeDistribution(dir_out_cnt, resultsTypeDir + "/dir_out_cnt.txt");
        writeDistribution(dir_out_rev, resultsTypeDir + "/dir_out_rev.txt");
        writeDistribution(rev_in_dir, resultsTypeDir + "/rev_in_dir.txt");
        writeDistribution(rev_in_rel, resultsTypeDir + "/rev_in_rel.txt");
        writeDistribution(rev_in_rev, resultsTypeDir + "/rev_in_rev.txt");
        writeDistribution(rev_in_snp, resultsTypeDir + "/rev_in_snp.txt");
        writeDistribution(rev_in_all, resultsTypeDir + "/rev_in_all.txt");
        writeDistribution(rev_out_rev, resultsTypeDir + "/rev_out_rev.txt");
        writeDistribution(rel_in_snp, resultsTypeDir + "/rel_in_snp.txt");
        writeDistribution(snp_in_ori, resultsTypeDir + "/snp_in_ori.txt");
        writeDistribution(snp_out_all, resultsTypeDir + "/snp_out_all.txt");
        writeDistribution(snp_out_rel, resultsTypeDir + "/snp_out_rel.txt");
        writeDistribution(snp_out_rev, resultsTypeDir + "/snp_out_rev.txt");
        writeDistribution(ori_out_snp, resultsTypeDir + "/ori_out_snp.txt");

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

        SwhBidirectionalGraph graph = SwhBidirectionalGraph.loadMapped(basename);
        run(graph, resultsDir);
    }
}

/*
 * Copyright (c) 2021-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.File;
import java.io.IOException;

/**
 * CLI program used to compose two on-disk permutations.
 *
 * It takes two on-disk permutations as parameters, p1 and p2, and writes on disk (p1 o p2) at the
 * given location. This is useful for multi-step compression (e.g., Unordered -> BFS -> LLP), as it
 * can be used to merge all the intermediate permutations.
 */
public class ComposePermutations {
    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ComposePermutations.class.getName(), "", new Parameter[]{
                    new UnflaggedOption("firstPermutation", JSAP.STRING_PARSER, JSAP.REQUIRED, "The first permutation"),
                    new UnflaggedOption("secondPermutation", JSAP.STRING_PARSER, JSAP.REQUIRED,
                            "The second permutation"),
                    new UnflaggedOption("outputPermutation", JSAP.STRING_PARSER, JSAP.REQUIRED,
                            "The output permutation"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        JSAPResult config = parse_args(args);
        String firstPermFilename = config.getString("firstPermutation");
        String secondPermFilename = config.getString("secondPermutation");
        String outputPermFilename = config.getString("outputPermutation");

        long[][] firstPerm = BinIO.loadLongsBig(new File(firstPermFilename));
        long[][] secondPerm = BinIO.loadLongsBig(new File(secondPermFilename));

        long[][] outputPerm = Util.composePermutationsInPlace(firstPerm, secondPerm);

        BinIO.storeLongs(outputPerm, outputPermFilename);
    }
}

/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

// Batch converter for MPH files from the .mph format (used by WebGraph-java)
// to the .cmph format (used by webgraph-rs).
//
// A oneliner CLI equivalent of this using jshell is:
//
// echo '((it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction) it.unimi.dsi.fastutil.io.BinIO.loadObject("input.mph")).dump("output.cmph");' | jshell --class-path /path/to/swh-graph.jar

package org.softwareheritage.graph.utils;

import java.io.IOException;

public class Mph2Cmph {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        if (args.length != 2) {
            System.err.println("Usage: Mph2Cmph INPUT.mph OUTPUT.cmph");
            System.exit(2);
        }
        String inputMphPath = args[0];
        String outputCmphPath = args[1];

        System.out.println("Converting MPH file " + inputMphPath + " to CMPH file " + outputCmphPath + " ...");
        ((it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction) it.unimi.dsi.fastutil.io.BinIO.loadObject(inputMphPath))
                .dump(outputCmphPath);
        System.out.println("Done.");
    }

}

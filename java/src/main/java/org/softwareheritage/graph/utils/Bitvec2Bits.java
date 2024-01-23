/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

// Converter for serialized BooleanBigArrayBigList (used by swh-graph-java)
// to the .bits format (used by swh-graph-rs).

package org.softwareheritage.graph.utils;

import java.io.IOException;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.bits.LongArrayBitVector;

public class Bitvec2Bits {

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        if (args.length != 2) {
            System.err.println("Usage: Bitvec2Bits INPUT.bin OUTPUT.bits");
            System.exit(2);
        }
        String inputBinPath = args[0];
        String outputBitsPath = args[1];

        System.out.println("Reading LongArrayBitVector file " + inputBinPath + " ...");
        LongArrayBitVector bitvec = (LongArrayBitVector) BinIO.loadObject(inputBinPath);

        System.out.println("Dumping bits file " + outputBitsPath + " ...");
        BinIO.storeLongs(bitvec.bits(), outputBitsPath);

        System.out.println("Done.");
    }

}

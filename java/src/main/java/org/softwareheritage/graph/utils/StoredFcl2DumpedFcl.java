/*
 * Copyright (c) 2024 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import com.martiansoftware.jsap.*;
import com.martiansoftware.jsap.stringparsers.*;
import it.unimi.dsi.big.util.FrontCodedStringBigList;
import it.unimi.dsi.fastutil.io.BinIO;
import org.apache.commons.configuration2.ex.ConfigurationException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoredFcl2DumpedFcl {

    public static void main(final String[] arg)
            throws JSAPException, IOException, ConfigurationException, ClassNotFoundException {
        final SimpleJSAP jsap = new SimpleJSAP(StoredFcl2DumpedFcl.class.getName(),
                "Reads a .fcl file and produces .fcl.bytearray, .fcl.pointers, and .fcl.properties file which can be mmapped",
                new Parameter[]{
                        new FlaggedOption("ratio", IntSizeStringParser.getParser(), "4", JSAP.NOT_REQUIRED, 'r',
                                "ratio", "The compression ratio."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                JSAP.NOT_GREEDY,
                                "The basename of the files associated with the memory-mapped front-coded string list.")});

        final JSAPResult args = jsap.parse(arg);
        if (jsap.messagePrinted())
            return;

        final String basename = args.getString("basename");

        final Logger logger = LoggerFactory.getLogger(StoredFcl2DumpedFcl.class);

        logger.info("Reading front-coded string big list...");
        final FrontCodedStringBigList nonutf8Fcl = (FrontCodedStringBigList) BinIO.loadObject(System.in);
        final FrontCodedStringBigList utf8Fcl = new FrontCodedStringBigList(nonutf8Fcl, args.getInt("ratio"), true);

        logger.info("Dumping files...");
        utf8Fcl.dump(basename);
    }
}

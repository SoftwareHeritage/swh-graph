/*
 * Copyright (c) 2020 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import org.softwareheritage.graph.maps.NodeIdMap;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class MPHTranslate {
    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(MPHTranslate.class.getName(), "",
                    new Parameter[]{new UnflaggedOption("function", JSAP.STRING_PARSER, JSAP.REQUIRED,
                            "Filename of the serialized MPH"),});

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
        String mphPath = config.getString("function");

        Object2LongFunction<byte[]> mphMap = NodeIdMap.loadMph(mphPath);

        // TODO: wasteful to convert to/from bytes
        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII));
        LineIterator lineIterator = new LineIterator(buffer);

        while (lineIterator.hasNext()) {
            String line = lineIterator.next().toString();
            System.out.println(mphMap.getLong(line.getBytes(StandardCharsets.US_ASCII)));
        }
    }
}

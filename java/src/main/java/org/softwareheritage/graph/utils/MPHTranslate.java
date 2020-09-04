package org.softwareheritage.graph.utils;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import org.softwareheritage.graph.experiments.multiplicationfactor.GenDistribution;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class MPHTranslate {
    final static String SORT_BUFFER_SIZE = "40%";

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(
                    GenDistribution.class.getName(),
                    "",
                    new Parameter[]{
                            new UnflaggedOption("function", JSAP.STRING_PARSER, JSAP.REQUIRED,
                                    "Filename of the serialized MPH"),
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
        String mphPath = config.getString("function");
        outputPermutation(mphPath);
    }


    @SuppressWarnings("unchecked") // Suppress warning for Object2LongFunction cast
    static void outputPermutation(String mphPath)
            throws IOException {
        Object2LongFunction<String> mphMap = null;
        try {
            mphMap = (Object2LongFunction<String>) BinIO.loadObject(mphPath);
        } catch (ClassNotFoundException e) {
            System.exit(2);
        }

        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(System.in,
                StandardCharsets.US_ASCII));
        LineIterator lineIterator = new LineIterator(buffer);

        while (lineIterator.hasNext()) {
            String line = lineIterator.next().toString();
            System.out.println(mphMap.getLong(line));
        }
    }
}

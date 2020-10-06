package org.softwareheritage.graph.utils;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;

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

    @SuppressWarnings("unchecked") // Suppress warning for Object2LongFunction cast
    static Object2LongFunction<String> loadMPH(String mphPath) throws IOException, ClassNotFoundException {
        return (Object2LongFunction<String>) BinIO.loadObject(mphPath);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        JSAPResult config = parse_args(args);
        String mphPath = config.getString("function");

        Object2LongFunction<String> mphMap = loadMPH(mphPath);

        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII));
        LineIterator lineIterator = new LineIterator(buffer);

        while (lineIterator.hasNext()) {
            String line = lineIterator.next().toString();
            System.out.println(mphMap.getLong(line));
        }
    }
}

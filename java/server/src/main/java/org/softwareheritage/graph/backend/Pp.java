package org.softwareheritage.graph.backend;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.zip.GZIPInputStream;

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.backend.NodeTypesMap;

public class Pp {

    public static void main(String[] args) throws IOException {

        Object2LongFunction<String> mphMap = null;
        try {
            mphMap = (Object2LongFunction<String>) BinIO.loadObject("all.mph");
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("The .mph file contains unknown class object: " + e);
        }

        long nbIds = (mphMap instanceof Size64) ? ((Size64) mphMap).size64() : mphMap.size();

        System.out.println("mph size: " + nbIds);
    }
}

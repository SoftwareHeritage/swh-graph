package org.softwareheritage.graph.backend;

import java.io.IOException;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;

public class Pp {
    @SuppressWarnings("unchecked")
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

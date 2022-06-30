/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongArrays;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ForkJoinBigQuickSort2Test {
    private static long[] identity(final int n) {
        final long[] perm = new long[n];
        for (int i = perm.length; i-- != 0;)
            perm[i] = i;
        return perm;
    }

    private static void checkArraySorted(long[] x, long[] y) {
        checkArraySorted(x, y, 0, x.length);
    }

    private static void checkArraySorted(long[] x, long[] y, int from, int to) {
        for (int i = to - 1; i-- != from;)
            assertTrue(x[i] < x[i + 1] || x[i] == x[i + 1] && (y[i] < y[i + 1] || y[i] == y[i + 1]),
                    String.format("%d: <%d, %d>, <%d, %d>", i, x[i], y[i], x[i + 1], y[i + 1]));
    }

    private static void sortBig2(long[] x, long[] y, long from, long to) {
        ForkJoinBigQuickSort2.parallelQuickSort(BigArrays.wrap(x), BigArrays.wrap(y), from, to);
    }

    private static void sortBig2(long[] x, long[] y) {
        sortBig2(x, y, 0, x.length);
    }

    @Test
    public void testParallelQuickSort3() {
        final long[][] d = new long[2][];

        d[0] = new long[10];
        for (int i = d[0].length; i-- != 0;)
            d[0][i] = 3 - i % 3;
        d[1] = LongArrays.shuffle(identity(10), new Random(0));
        sortBig2(d[0], d[1]);
        checkArraySorted(d[0], d[1]);

        d[0] = new long[100000];
        for (int i = d[0].length; i-- != 0;)
            d[0][i] = 100 - i % 100;
        d[1] = LongArrays.shuffle(identity(100000), new Random(6));
        sortBig2(d[0], d[1]);
        checkArraySorted(d[0], d[1]);

        d[0] = new long[10];
        for (int i = d[0].length; i-- != 0;)
            d[0][i] = i % 3 - 2;
        Random random = new Random(0);
        d[1] = new long[d[0].length];
        for (int i = d[1].length; i-- != 0;)
            d[1][i] = random.nextInt();
        sortBig2(d[0], d[1]);
        checkArraySorted(d[0], d[1]);

        d[0] = new long[100000];
        d[1] = new long[100000];
        sortBig2(d[0], d[1]);
        checkArraySorted(d[0], d[1]);

        d[0] = new long[100000];
        random = new Random(0);
        for (int i = d[0].length; i-- != 0;)
            d[0][i] = random.nextInt();
        d[1] = new long[d[0].length];
        for (int i = d[1].length; i-- != 0;)
            d[1][i] = random.nextInt();
        sortBig2(d[0], d[1]);
        checkArraySorted(d[0], d[1]);
        for (int i = 100; i-- != 10;)
            d[0][i] = random.nextInt();
        for (int i = 100; i-- != 10;)
            d[1][i] = random.nextInt();
        sortBig2(d[0], d[1], 10, 100);
        checkArraySorted(d[0], d[1], 10, 100);
    }
}

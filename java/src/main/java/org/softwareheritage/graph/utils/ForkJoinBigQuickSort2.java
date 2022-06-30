/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.fastutil.BigArrays;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class ForkJoinBigQuickSort2 extends RecursiveAction {
    private static final long serialVersionUID = 1L;
    private final long from;
    private final long to;
    private final long[][] x, y;

    private static final int QUICKSORT_NO_REC = 16;
    private static final int PARALLEL_QUICKSORT_NO_FORK = 8192;
    private static final int QUICKSORT_MEDIAN_OF_9 = 128;

    public ForkJoinBigQuickSort2(final long[][] x, final long[][] y, final long from, final long to) {
        this.from = from;
        this.to = to;
        this.x = x;
        this.y = y;
    }

    @Override
    protected void compute() {
        final long[][] x = this.x;
        final long[][] y = this.y;
        final long len = to - from;
        if (len < PARALLEL_QUICKSORT_NO_FORK) {
            quickSort(x, y, from, to);
            return;
        }
        // Choose a partition element, v
        long m = from + len / 2;
        long l = from;
        long n = to - 1;
        long s = len / 8;
        l = med3(x, y, l, l + s, l + 2 * s);
        m = med3(x, y, m - s, m, m + s);
        n = med3(x, y, n - 2 * s, n - s, n);
        m = med3(x, y, l, m, n);
        final long xm = BigArrays.get(x, m), ym = BigArrays.get(y, m);
        // Establish Invariant: v* (<v)* (>v)* v*
        long a = from, b = a, c = to - 1, d = c;
        while (true) {
            int comparison;
            while (b <= c && (comparison = compare(x, y, b, xm, ym)) <= 0) {
                if (comparison == 0)
                    swap(x, y, a++, b);
                b++;
            }
            while (c >= b && (comparison = compare(x, y, c, xm, ym)) >= 0) {
                if (comparison == 0)
                    swap(x, y, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(x, y, b++, c--);
        }
        // Swap partition elements back to middle
        long t;
        s = Math.min(a - from, b - a);
        swap(x, y, from, b - s, s);
        s = Math.min(d - c, to - d - 1);
        swap(x, y, b, to - s, s);
        s = b - a;
        t = d - c;
        // Recursively sort non-partition-elements
        if (s > 1 && t > 1)
            invokeAll(new ForkJoinBigQuickSort2(x, y, from, from + s), new ForkJoinBigQuickSort2(x, y, to - t, to));
        else if (s > 1)
            invokeAll(new ForkJoinBigQuickSort2(x, y, from, from + s));
        else
            invokeAll(new ForkJoinBigQuickSort2(x, y, to - t, to));
    }

    public static void quickSort(final long[][] x, final long[][] y, final long from, final long to) {
        final long len = to - from;
        if (len < QUICKSORT_NO_REC) {
            selectionSort(x, y, from, to);
            return;
        }
        // Choose a partition element, v
        long m = from + len / 2;
        long l = from;
        long n = to - 1;
        if (len > QUICKSORT_MEDIAN_OF_9) { // Big arrays, pseudomedian of 9
            long s = len / 8;
            l = med3(x, y, l, l + s, l + 2 * s);
            m = med3(x, y, m - s, m, m + s);
            n = med3(x, y, n - 2 * s, n - s, n);
        }
        m = med3(x, y, l, m, n); // Mid-size, med of 3
        // Establish Invariant: v* (<v)* (>v)* v*
        long a = from, b = a, c = to - 1, d = c;
        final long xm = BigArrays.get(x, m), ym = BigArrays.get(y, m);
        while (true) {
            long comparison;
            while (b <= c && (comparison = compare(x, y, b, xm, ym)) <= 0) {
                if (comparison == 0)
                    swap(x, y, a++, b);
                b++;
            }
            while (c >= b && (comparison = compare(x, y, c, xm, ym)) >= 0) {
                if (comparison == 0)
                    swap(x, y, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(x, y, b++, c--);
        }
        // Swap partition elements back to middle
        long s;
        s = Math.min(a - from, b - a);
        swap(x, y, from, b - s, s);
        s = Math.min(d - c, to - d - 1);
        swap(x, y, b, to - s, s);
        // Recursively sort non-partition-elements
        if ((s = b - a) > 1)
            quickSort(x, y, from, from + s);
        if ((s = d - c) > 1)
            quickSort(x, y, to - s, to);
    }

    public static void quickSort(final long[][] x, final long[][] y) {
        quickSort(x, y, 0, x.length);
    }

    private static int compare(final long[][] x, final long[][] y, final long u, final long v) {
        int tx;
        return (tx = Long.compare(BigArrays.get(x, u), BigArrays.get(x, v))) != 0
                ? tx
                : Long.compare(BigArrays.get(y, u), BigArrays.get(y, v));
    }

    private static int compare(final long[][] x, final long[][] y, final long i, final long xm, final long ym) {
        int tx;
        return (tx = Long.compare(BigArrays.get(x, i), xm)) != 0 ? tx : Long.compare(BigArrays.get(y, i), ym);
    }

    private static void swap(final long[][] x, final long[][] y, final long a, final long b) {
        BigArrays.swap(x, a, b);
        BigArrays.swap(y, a, b);
    }

    private static void swap(final long[][] x, final long[][] y, long a, long b, final long n) {
        for (long i = 0; i < n; i++, a++, b++)
            swap(x, y, a, b);
    }

    private static long med3(final long[][] x, final long[][] y, final long a, final long b, final long c) {
        final int ab = compare(x, y, a, b);
        final int ac = compare(x, y, a, c);
        final int bc = compare(x, y, b, c);
        return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
    }

    public static void selectionSort(final long[][] a, final long[][] b, final long from, final long to) {
        for (long i = from; i < to - 1; i++) {
            long m = i;
            for (long j = i + 1; j < to; j++)
                if (compare(a, b, j, m) < 0)
                    m = j;
            if (m != i) {
                BigArrays.swap(a, i, m);
                BigArrays.swap(b, i, m);
            }
        }
    }

    public static void selectionSort(final long[][] x, final long[][] y) {
        selectionSort(x, y, 0, x.length);
    }

    public static ForkJoinPool getPool() {
        ForkJoinPool current = ForkJoinTask.getPool();
        return current == null ? ForkJoinPool.commonPool() : current;
    }

    public static void parallelQuickSort(final long[][] x, final long[][] y) {
        BigArrays.ensureSameLength(x, y);
        parallelQuickSort(x, y, 0, x.length);
    }

    public static void parallelQuickSort(final long[][] x, final long[][] y, final long from, final long to) {
        ForkJoinPool pool = getPool();
        if (to - from < PARALLEL_QUICKSORT_NO_FORK || pool.getParallelism() == 1)
            quickSort(x, y, from, to);
        else {
            pool.invoke(new ForkJoinBigQuickSort2(x, y, from, to));
        }
    }
}

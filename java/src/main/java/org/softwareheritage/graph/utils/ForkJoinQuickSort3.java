/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

import static it.unimi.dsi.fastutil.longs.LongArrays.ensureSameLength;

public class ForkJoinQuickSort3 extends RecursiveAction {
    private static final long serialVersionUID = 1L;
    private final int from;
    private final int to;
    private final long[] x, y, z;

    private static final int QUICKSORT_NO_REC = 16;
    private static final int PARALLEL_QUICKSORT_NO_FORK = 8192;
    private static final int QUICKSORT_MEDIAN_OF_9 = 128;

    public ForkJoinQuickSort3(final long[] x, final long[] y, final long z[], final int from, final int to) {
        this.from = from;
        this.to = to;
        this.x = x;
        this.y = y;
        this.z = z;
    }

    @Override
    protected void compute() {
        final long[] x = this.x;
        final long[] y = this.y;
        final long[] z = this.z;
        final int len = to - from;
        if (len < PARALLEL_QUICKSORT_NO_FORK) {
            quickSort(x, y, z, from, to);
            return;
        }
        // Choose a partition element, v
        int m = from + len / 2;
        int l = from;
        int n = to - 1;
        int s = len / 8;
        l = med3(x, y, z, l, l + s, l + 2 * s);
        m = med3(x, y, z, m - s, m, m + s);
        n = med3(x, y, z, n - 2 * s, n - s, n);
        m = med3(x, y, z, l, m, n);
        final long xm = x[m], ym = y[m], zm = z[m];
        // Establish Invariant: v* (<v)* (>v)* v*
        int a = from, b = a, c = to - 1, d = c;
        while (true) {
            int comparison, t;
            while (b <= c && (comparison = compare(x, y, z, b, xm, ym, zm)) <= 0) {
                if (comparison == 0)
                    swap(x, y, z, a++, b);
                b++;
            }
            while (c >= b && (comparison = compare(x, y, z, c, xm, ym, zm)) >= 0) {
                if (comparison == 0)
                    swap(x, y, z, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(x, y, z, b++, c--);
        }
        // Swap partition elements back to middle
        int t;
        s = Math.min(a - from, b - a);
        swap(x, y, z, from, b - s, s);
        s = Math.min(d - c, to - d - 1);
        swap(x, y, z, b, to - s, s);
        s = b - a;
        t = d - c;
        // Recursively sort non-partition-elements
        if (s > 1 && t > 1)
            invokeAll(new ForkJoinQuickSort3(x, y, z, from, from + s), new ForkJoinQuickSort3(x, y, z, to - t, to));
        else if (s > 1)
            invokeAll(new ForkJoinQuickSort3(x, y, z, from, from + s));
        else
            invokeAll(new ForkJoinQuickSort3(x, y, z, to - t, to));
    }

    public static void quickSort(final long[] x, final long[] y, final long[] z, final int from, final int to) {
        final int len = to - from;
        if (len < QUICKSORT_NO_REC) {
            selectionSort(x, y, z, from, to);
            return;
        }
        // Choose a partition element, v
        int m = from + len / 2;
        int l = from;
        int n = to - 1;
        if (len > QUICKSORT_MEDIAN_OF_9) { // Big arrays, pseudomedian of 9
            int s = len / 8;
            l = med3(x, y, z, l, l + s, l + 2 * s);
            m = med3(x, y, z, m - s, m, m + s);
            n = med3(x, y, z, n - 2 * s, n - s, n);
        }
        m = med3(x, y, z, l, m, n); // Mid-size, med of 3
        // Establish Invariant: v* (<v)* (>v)* v*
        int a = from, b = a, c = to - 1, d = c;
        final long xm = x[m], ym = y[m], zm = z[m];
        while (true) {
            int comparison;
            while (b <= c && (comparison = compare(x, y, z, b, xm, ym, zm)) <= 0) {
                if (comparison == 0)
                    swap(x, y, z, a++, b);
                b++;
            }
            while (c >= b && (comparison = compare(x, y, z, c, xm, ym, zm)) >= 0) {
                if (comparison == 0)
                    swap(x, y, z, c, d--);
                c--;
            }
            if (b > c)
                break;
            swap(x, y, z, b++, c--);
        }
        // Swap partition elements back to middle
        int s;
        s = Math.min(a - from, b - a);
        swap(x, y, z, from, b - s, s);
        s = Math.min(d - c, to - d - 1);
        swap(x, y, z, b, to - s, s);
        // Recursively sort non-partition-elements
        if ((s = b - a) > 1)
            quickSort(x, y, z, from, from + s);
        if ((s = d - c) > 1)
            quickSort(x, y, z, to - s, to);
    }

    public static void quickSort(final long[] x, final long[] y, final long[] z) {
        quickSort(x, y, z, 0, x.length);
    }

    private static int compare(final long[] x, final long[] y, final long[] z, final int u, final int v) {
        int tx, ty;
        return (tx = Long.compare(x[u], x[v])) != 0
                ? tx
                : ((ty = Long.compare(y[u], y[v])) != 0 ? ty : Long.compare(z[u], z[v]));
    }

    private static int compare(final long[] x, final long[] y, final long[] z, final int i, final long xm,
            final long ym, final long zm) {
        int tx, ty;
        return (tx = Long.compare(x[i], xm)) != 0
                ? tx
                : ((ty = Long.compare(y[i], ym)) != 0 ? ty : Long.compare(z[i], zm));
    }

    private static void swap(final long[] x, final long[] y, final long[] z, final int a, final int b) {
        final long t = x[a];
        final long u = y[a];
        final long v = z[a];
        x[a] = x[b];
        y[a] = y[b];
        z[a] = z[b];
        x[b] = t;
        y[b] = u;
        z[b] = v;
    }

    private static void swap(final long[] x, final long[] y, final long[] z, int a, int b, final int n) {
        for (int i = 0; i < n; i++, a++, b++)
            swap(x, y, z, a, b);
    }

    private static int med3(final long[] x, final long[] y, final long[] z, final int a, final int b, final int c) {
        final int ab = compare(x, y, z, a, b);
        final int ac = compare(x, y, z, a, c);
        final int bc = compare(x, y, z, b, c);
        return (ab < 0 ? (bc < 0 ? b : ac < 0 ? c : a) : (bc > 0 ? b : ac > 0 ? c : a));
    }

    public static void selectionSort(final long[] a, final long[] b, long[] c, final int from, final int to) {
        for (int i = from; i < to - 1; i++) {
            int m = i;
            for (int j = i + 1; j < to; j++)
                if (compare(a, b, c, j, m) < 0)
                    m = j;
            if (m != i) {
                long t = a[i];
                a[i] = a[m];
                a[m] = t;
                t = b[i];
                b[i] = b[m];
                b[m] = t;
                t = c[i];
                c[i] = c[m];
                c[m] = t;
            }
        }
    }

    public static void selectionSort(final long[] x, final long[] y, final long[] z) {
        selectionSort(x, y, z, 0, x.length);
    }

    public static ForkJoinPool getPool() {
        ForkJoinPool current = ForkJoinTask.getPool();
        return current == null ? ForkJoinPool.commonPool() : current;
    }

    public static void parallelQuickSort(final long[] x, final long[] y, final long[] z) {
        ensureSameLength(x, y);
        ensureSameLength(x, z);
        parallelQuickSort(x, y, z, 0, x.length);
    }

    public static void parallelQuickSort(final long[] x, final long[] y, final long[] z, final int from, final int to) {
        ForkJoinPool pool = getPool();
        if (to - from < PARALLEL_QUICKSORT_NO_FORK || pool.getParallelism() == 1)
            quickSort(x, y, z, from, to);
        else {
            pool.invoke(new ForkJoinQuickSort3(x, y, z, from, to));
        }
    }
}

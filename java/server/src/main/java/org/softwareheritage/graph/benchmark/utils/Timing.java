package org.softwareheritage.graph.benchmark.utils;

/**
 * Time measurement utility class.
 *
 * @author The Software Heritage developers
 */

public class Timing {
    /**
     * Returns measurement starting timestamp.
     *
     * @return timestamp used for time measurement
     */
    public static long start() {
        return System.nanoTime();
    }

    /**
     * Ends timing measurement and returns total duration in seconds.
     *
     * @param startTime measurement starting timestamp
     * @return time in seconds elapsed since starting point
     */
    public static double stop(long startTime) {
        long endTime = System.nanoTime();
        double duration = (double) (endTime - startTime) / 1_000_000_000;
        return duration;
    }
}

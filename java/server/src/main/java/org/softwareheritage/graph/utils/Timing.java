package org.softwareheritage.graph.utils;

/**
 * Time measurement utility class.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
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

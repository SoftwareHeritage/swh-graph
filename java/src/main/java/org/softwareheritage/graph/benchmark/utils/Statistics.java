package org.softwareheritage.graph.benchmark.utils;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Compute various statistics on a list of values.
 *
 * @author The Software Heritage developers
 */

public class Statistics {
    /** Input values */
    ArrayList<Double> values;

    /**
     * Constructor.
     *
     * @param values input values
     */
    public Statistics(ArrayList<Double> values) {
        this.values = values;
    }

    /**
     * Returns the minimum value.
     *
     * @return minimum value
     */
    public double getMin() {
        double min = Double.POSITIVE_INFINITY;
        for (double v : values) {
            min = Math.min(min, v);
        }
        return min;
    }

    /**
     * Returns the maximum value.
     *
     * @return maximum value
     */
    public double getMax() {
        double max = Double.NEGATIVE_INFINITY;
        for (double v : values) {
            max = Math.max(max, v);
        }
        return max;
    }

    /**
     * Computes the average.
     *
     * @return average value
     */
    public double getAverage() {
        double sum = 0;
        for (double v : values) {
            sum += v;
        }
        return sum / (double) values.size();
    }

    /**
     * Returns the median value.
     *
     * @return median value
     */
    public double getMedian() {
        Collections.sort(values);
        int length = values.size();
        if (length % 2 == 0) {
            return (values.get(length / 2) + values.get(length / 2 - 1)) / 2;
        } else {
            return values.get(length / 2);
        }
    }

    /**
     * Computes the standard deviation.
     *
     * @return standard deviation value
     */
    public double getStandardDeviation() {
        double average = getAverage();
        double variance = 0;
        for (double v : values) {
            variance += (v - average) * (v - average);
        }
        variance /= (double) values.size();
        return Math.sqrt(variance);
    }

    /**
     * Computes and prints all statistical values.
     */
    public void printAll() {
        System.out.println("min value: " + getMin());
        System.out.println("max value: " + getMax());
        System.out.println("average: " + getAverage());
        System.out.println("median: " + getMedian());
        System.out.println("standard deviation: " + getStandardDeviation());
    }
}

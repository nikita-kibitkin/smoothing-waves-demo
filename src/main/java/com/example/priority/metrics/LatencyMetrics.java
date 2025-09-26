package com.example.priority.metrics;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.springframework.stereotype.Component;

@Component
public class LatencyMetrics {
    private final static Histogram latencyHist = new ConcurrentHistogram(5_000_000L, 3);

    public static Histogram getHistogram() {
        return latencyHist;
    }

    public static Histogram getHistogramCopy() {
        return latencyHist.copy();
    }

}

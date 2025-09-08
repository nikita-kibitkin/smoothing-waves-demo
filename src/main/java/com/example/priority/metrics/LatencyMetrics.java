package com.example.priority.metrics;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.springframework.stereotype.Component;

@Component
public class LatencyMetrics {
    private final static Histogram highLatencyHist = new ConcurrentHistogram(500_000L, 3);
    private final static Histogram bulkLatencyHist = new ConcurrentHistogram(500_000L, 3);

    public static Histogram getHighHistogram() {
        return highLatencyHist;
    }

    public static Histogram getBulkHistogram() {
        return bulkLatencyHist;
    }

    public static Histogram getHighHistogramCopy() {
        return highLatencyHist.copy();
    }

    public static Histogram getBulkHistogramCopy() {
        return bulkLatencyHist.copy();
    }
}

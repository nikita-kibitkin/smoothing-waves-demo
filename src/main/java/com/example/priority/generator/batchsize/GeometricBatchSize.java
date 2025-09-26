package com.example.priority.generator.batchsize;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadLocalRandom;

/** Geometric(p) with mean ~= 1/p ; configured via mean for convenience. */
@Slf4j
public final class GeometricBatchSize implements BatchSizeSampler {
    private final double p; // in (0,1]
    private GeometricBatchSize(double p) {
        if (!(p > 0.0) || p > 1.0) throw new IllegalArgumentException("p must be in (0,1]");
        this.p = p;
    }
    public static GeometricBatchSize ofMean(double mean) {
        if (!(mean >= 1.0)) throw new IllegalArgumentException("mean must be >= 1");
        double p = 1.0 / mean; // mean ≈ 1/p for k>=1 variant
        return new GeometricBatchSize(p);
    }
    @Override
    public int sample() {
        if (p >= 1.0) return 1; // mean=1 → всегда 1
        // Численно устойчивее использовать log1p
        double u = ThreadLocalRandom.current().nextDouble(); // (0,1)
        // ln(1 - u) / ln(1 - p) > 0; добавляем 1 для сдвинутой геометрической
        int k = 1 + (int) Math.floor(Math.log1p(-u) / Math.log1p(-p));
        var sample = Math.max(1, k);
        log.info("sample: {}", sample);
        return sample;
    }
}

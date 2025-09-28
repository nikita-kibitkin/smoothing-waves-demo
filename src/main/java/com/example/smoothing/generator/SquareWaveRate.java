package com.example.smoothing.generator;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Square wave LOW/HIGH rate with optional multiplicative jitter (raggedness).
 */
@Slf4j
public final class SquareWaveRate implements RateFunction {
    private final double low;
    private final double high;
    private final Duration period;
    private final double dutyCycle; // fraction of period in HIGH state (0..1)
    private final Instant phase;
    private final double jitterFrac; // 0..1 multiplicative jitter

    private SquareWaveRate(double low, double high, Duration period, double dutyCycle, Instant phase, double jitterFrac) {
        if (!(low > 0.0) || !(high > 0.0)) throw new IllegalArgumentException("low/high must be > 0");
        if (high < low) throw new IllegalArgumentException("high must be >= low");
        if (period == null || period.isZero() || period.isNegative())
            throw new IllegalArgumentException("period must be > 0");
        if (dutyCycle < 0.0 || dutyCycle > 1.0) throw new IllegalArgumentException("dutyCycle must be in [0,1]");
        if (jitterFrac < 0.0 || jitterFrac > 1.0) throw new IllegalArgumentException("jitterFrac must be in [0,1]");
        this.low = low;
        this.high = high;
        this.period = period;
        this.dutyCycle = dutyCycle;
        this.phase = (phase != null) ? phase : Instant.EPOCH;
        this.jitterFrac = jitterFrac;
    }

    public static SquareWaveRate of(double low, double high, Duration period, double dutyCycle) {
        return new SquareWaveRate(low, high, period, dutyCycle, Instant.EPOCH, 0.0);
    }

    /**
     * 0..1 jitter fraction; e.g., 0.1 gives U[0.9,1.1] multiplier.
     */
    public SquareWaveRate withJitter(double jitterFrac) {
        return new SquareWaveRate(low, high, period, dutyCycle, phase, jitterFrac);
    }

    @Override
    public double ratePerSecond(Instant t) {
        long perNanos = period.toNanos();
        long sincePhase = Math.max(0L, t.minusMillis(0).toEpochMilli() - phase.toEpochMilli());
        long mod = Math.floorMod(sincePhase, period.toMillis());
        double frac = (perNanos == 0) ? 0.0 : (mod / (double) period.toMillis());
        //log.info("Frac = {}", frac);
        boolean isHigh = frac < dutyCycle;
        double base = isHigh ? high : low;
        if (jitterFrac == 0.0) return base;
        double a = 1.0 - jitterFrac;
        double b = 1.0 + jitterFrac;
        double mult = a + (b - a) * ThreadLocalRandom.current().nextDouble();
        var rate = Math.max(0.0, base * mult);
        //log.info("Rate per second: {}", rate);
        return rate;
    }

    @Override
    public double upperBoundRatePerSecond() {
        return high * (1.0 + jitterFrac);
    }
}
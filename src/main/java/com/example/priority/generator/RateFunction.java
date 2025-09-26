package com.example.priority.generator;

import java.time.Instant;

/** Time-varying rate: λ(t) in events/sec, with a known finite upper bound. */
public interface RateFunction {
    /** λ(t) in events/sec at an absolute time. Must be >= 0. */
    double ratePerSecond(Instant t);
    /** A global finite upper bound for λ(t), required by thinning. */
    double upperBoundRatePerSecond();
}

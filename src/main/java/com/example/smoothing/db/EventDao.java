package com.example.smoothing.db;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventDao {
    private final JdbcTemplate jdbc;
    private static final String SQL = "INSERT INTO events(generated_at_ms, payload_text) VALUES (?, ?)";

    /** Returns length of DB operation in ms. */
    public double insert(long generatedAtMs, String payloadText) {
        long t1 = System.nanoTime();
        jdbc.update(SQL, ps -> {
            ps.setLong(1, generatedAtMs);
            ps.setString(2, payloadText);
        });
        return (System.nanoTime() - t1) / 1_000_000.0;
    }
}
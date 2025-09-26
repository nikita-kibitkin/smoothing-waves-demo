package com.example.smoothing.model;

import java.io.Serializable;

public record Message(Long startTimeMs, String payload) implements Serializable {
}

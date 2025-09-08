package com.example.priority.model;

import java.io.Serializable;

public record Message(Long startTimeMs, boolean highPriority, String payload) implements Serializable {
}

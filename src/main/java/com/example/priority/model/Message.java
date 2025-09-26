package com.example.priority.model;

import java.io.Serializable;

public record Message(Long startTimeMs, String payload) implements Serializable {
}

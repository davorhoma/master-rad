package com.example.kafka_streams_examples;

import java.util.Arrays;
import java.util.List;

/** Čitanje konfiguracije iz environment varijabli. */
final class AppConfig {

    private AppConfig() {
    }

    /**
     * Vraća vrednost varijable ili defaultValue (može biti null) ako varijabla nije
     * postavljena/prazna.
     */
    static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isBlank()) ? defaultValue : value.trim();
    }

    static String required(String name) {
        String value = env(name, null);
        if (value == null) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return value;
    }

    static int envInt(String name, int defaultValue) {
        String value = env(name, null);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Environment variable " + name + " must be an integer, got: " + value, e);
        }
    }

    static List<String> envList(String name, String defaultValue) {
        return Arrays.stream(env(name, defaultValue).split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }
}
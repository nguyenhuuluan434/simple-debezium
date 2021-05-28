package com.example.debezium.demo.service;

import io.debezium.data.Envelope;

import java.util.Map;

public interface ConsumeEventService {
    void handleEvent(Map<String, Object> payload, Envelope.Operation operation);

}

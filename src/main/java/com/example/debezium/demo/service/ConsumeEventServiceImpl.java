package com.example.debezium.demo.service;

import io.debezium.data.Envelope;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class ConsumeEventServiceImpl implements ConsumeEventService {
    @Override
    public void handleEvent(Map<String, Object> payload, Envelope.Operation operation) {
        System.out.println(payload);
        System.out.println(operation);
    }
}

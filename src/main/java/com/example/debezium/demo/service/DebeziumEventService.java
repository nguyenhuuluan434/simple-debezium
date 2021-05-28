package com.example.debezium.demo.service;

import io.debezium.data.Envelope;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static io.debezium.data.Envelope.FieldName.*;
import static io.debezium.data.Envelope.Operation;

@Component
public class DebeziumEventService {
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    private final ConsumeEventService consumeEventService;
    private final Executor executor = Executors.newSingleThreadExecutor();

    public DebeziumEventService(@Qualifier("debeziumPropertiesConfiguration") io.debezium.config.Configuration debeziumConfiguration, ConsumeEventService consumeEventService) {
        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(debeziumConfiguration.asProperties())
                .notifying(this::handleChangeEvent)
                .build();

        this.consumeEventService = consumeEventService;
    }

    private void handleChangeEvent(RecordChangeEvent<SourceRecord> sourceRecordRecordChangeEvent) {
        SourceRecord sourceRecord = sourceRecordRecordChangeEvent.record();
        Struct sourceRecordChangeValue = (Struct) sourceRecord.value();
        if (sourceRecordChangeValue == null)
            return;
        Operation operation = Envelope.Operation.forCode((String) sourceRecordChangeValue.get(OPERATION));
        if (operation == Operation.READ)
            return;
        String record = operation == Operation.DELETE ? BEFORE : AFTER; // Handling Update & Insert operations.
        Struct struct = (Struct) sourceRecordChangeValue.get(record);
        Map<String, Object> payload = struct.schema().fields().stream()
                .map(Field::name)
                .filter(fieldName -> struct.get(fieldName) != null)
                .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        this.consumeEventService.handleEvent(payload, operation);

    }

    @PostConstruct
    private void start() {
        this.executor.execute(debeziumEngine);
    }

    @PreDestroy
    private void stop() throws IOException {
        if (this.debeziumEngine != null) {
            this.debeziumEngine.close();
        }
    }
}

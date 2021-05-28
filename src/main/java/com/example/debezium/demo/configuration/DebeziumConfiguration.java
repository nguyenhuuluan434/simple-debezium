package com.example.debezium.demo.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;

@Configuration
public class DebeziumConfiguration {
    @Value("${org.datasource.host}")
    private String orgDbHost;

    @Value("${org.datasource.database}")
    private String orgDbDatabase;

    @Value("${org.datasource.port}")
    private String orgDbPort;

    @Value("${org.datasource.username}")
    private String orgDbUserName;

    @Value("${org.datasource.password}")
    private String orgDbPassword;

    @Bean("debeziumPropertiesConfiguration")
    public io.debezium.config.Configuration debeziumPropertiesConfiguration() throws IOException {
        File offsetStorageTempFile = File.createTempFile("offsets_", ".dat");
        File dbHistoryTempFile = File.createTempFile("dbhistory_", ".dat");
        return io.debezium.config.Configuration.create()
                .with("name", "customer-mysql-connector")
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath())
                .with("offset.flush.interval.ms", "60000")
                .with("database.hostname", orgDbHost)
                .with("database.port", orgDbPort)
                .with("database.user", orgDbUserName)
                .with("database.password", orgDbPassword)
                .with("database.dbname", orgDbDatabase)
                .with("database.include.list", orgDbDatabase)
                .with("include.schema.changes", "false")
                .with("database.allowPublicKeyRetrieval", "true")
                .with("database.server.id", "10181")
                .with("database.server.name", "customer-mysql-db-server")
                .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename", dbHistoryTempFile.getAbsolutePath())
                .build();
    }
}

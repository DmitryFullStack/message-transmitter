package com.luxoft.kirilin.messagetransmitter.config.troutbox;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class ExtendedMigrationManager {
    private static final String OUTBOX_TABLE_PREPARED_STATEMENT = "create table if not exists %s " +
            "(id varchar(36) not null primary key,"
            + "payload         text,"
            + "approved        boolean not null default false,"
            + "partition        integer,"
            + "create_time timestamp(6));";
    private static final String OUTBOX_TABLE_INDEX_PREPARED_STATEMENT = "create index if not exists %s " +
            "on %s (partition, create_time);";

    static void migrate(DataSource dataSource, String tableName) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement createTable = connection.prepareStatement(String.format(OUTBOX_TABLE_PREPARED_STATEMENT, tableName));
             PreparedStatement createIndex = connection.prepareStatement(String.format(OUTBOX_TABLE_INDEX_PREPARED_STATEMENT, "index_" + tableName, tableName))) {
            connection.setAutoCommit(false);
            createTable.executeUpdate();
            createIndex.executeUpdate();
            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException("Migrations failed", e);
        }
    }
}

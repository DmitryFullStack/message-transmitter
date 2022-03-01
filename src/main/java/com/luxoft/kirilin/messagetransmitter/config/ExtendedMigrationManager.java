package com.luxoft.kirilin.messagetransmitter.config;

import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.TransactionManager;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class ExtendedMigrationManager {
    private List<ExtendedMigrationManager.Migration> MIGRATIONS;
    private String tableName;

    void migrate(TransactionManager transactionManager, @NotNull Dialect dialect) {
        transactionManager.inTransaction(
                transaction -> {
                    try {
                        int currentVersion = currentVersion(transaction.connection());
                        MIGRATIONS.stream()
                                .filter(migration -> migration.version > currentVersion)
                                .forEach(migration -> runSql(transaction.connection(), migration, dialect));
                    } catch (Exception e) {
                        throw new RuntimeException("Migrations failed", e);
                    }
                });
    }

    protected static ExtendedMigrationManager getManager(String tableName){
        ExtendedMigrationManager extendedMigrationManager = new ExtendedMigrationManager();
        extendedMigrationManager.MIGRATIONS =
                List.of(
                        new ExtendedMigrationManager.Migration(
                                1,
                                "Create outbox table",
                                "CREATE TABLE " + tableName + " (\n"
                                        + "    id VARCHAR(36) PRIMARY KEY,\n"
                                        + "    invocation TEXT,\n"
                                        + "    nextAttemptTime TIMESTAMP(6),\n"
                                        + "    attempts INT,\n"
                                        + "    blacklisted BOOLEAN,\n"
                                        + "    version INT\n"
                                        + ")"),
                        new ExtendedMigrationManager.Migration(
                                2,
                                "Add unique request id",
                                "ALTER TABLE " + tableName + " ADD COLUMN uniqueRequestId VARCHAR(100) NULL UNIQUE"),
                        new ExtendedMigrationManager.Migration(
                                3, "Add processed flag", "ALTER TABLE " + tableName + " ADD COLUMN processed BOOLEAN"),
                        new ExtendedMigrationManager.Migration(
                                4,
                                "Add flush index",
                                "CREATE INDEX IX_" + tableName + "_1 ON " + tableName + " (processed, blacklisted, nextAttemptTime)"),
                        new ExtendedMigrationManager.Migration(
                                5,
                                "Increase size of uniqueRequestId",
                                "ALTER TABLE " + tableName + " MODIFY COLUMN uniqueRequestId VARCHAR(250)",
                                Map.of(
                                        Dialect.POSTGRESQL_9,
                                        "ALTER TABLE " + tableName + " ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)",
                                        Dialect.H2,
                                        "ALTER TABLE " + tableName + " ALTER COLUMN uniqueRequestId VARCHAR(250)")),
                        new ExtendedMigrationManager.Migration(
                                6,
                                "Rename column blacklisted to blocked",
                                "ALTER TABLE " + tableName + " CHANGE COLUMN blacklisted blocked VARCHAR(250)",
                                Map.of(
                                        Dialect.POSTGRESQL_9,
                                        "ALTER TABLE " + tableName + " RENAME COLUMN blacklisted TO blocked",
                                        Dialect.H2,
                                        "ALTER TABLE " + tableName + " RENAME COLUMN blacklisted TO blocked")),
                        new ExtendedMigrationManager.Migration(
                                7,
                                "Add lastAttemptTime column to outbox",
                                "ALTER TABLE " + tableName + " ADD COLUMN lastAttemptTime TIMESTAMP(6) NULL AFTER invocation",
                                Map.of(
                                        Dialect.POSTGRESQL_9,
                                        "ALTER TABLE " + tableName + " ADD COLUMN lastAttemptTime TIMESTAMP(6)")),
                        new ExtendedMigrationManager.Migration(
                                8,
                                "Update length of invocation column on outbox for MySQL dialects only.",
                                "ALTER TABLE " + tableName + " MODIFY COLUMN invocation MEDIUMTEXT",
                                Map.of(Dialect.POSTGRESQL_9, "", Dialect.H2, "")));
        extendedMigrationManager.tableName = tableName;
        return extendedMigrationManager;
    }

    @SneakyThrows
    private void runSql(Connection connection, ExtendedMigrationManager.Migration migration, @NotNull Dialect dialect) {
        log.info("Running migration: {}", migration.name);
        try (Statement s = connection.createStatement()) {
            s.execute(migration.sqlFor(dialect));
            if (s.executeUpdate("UPDATE TXNO_VERSION SET version = " + migration.version + " WHERE table_name = '" + this.tableName + "';") != 1) {
                s.execute("INSERT INTO TXNO_VERSION VALUES ('" + this.tableName + "', " + migration.version + ");");
            }
        }
    }

    private int currentVersion(Connection connection) throws SQLException {
        createVersionTableIfNotExists(connection);
        try (PreparedStatement ps = connection.prepareStatement("SELECT version FROM TXNO_VERSION WHERE table_name = ?")){
            ps.setString(1, this.tableName);
            ResultSet resultSet = ps.executeQuery();
            if (!resultSet.next()) {
                return 0;
            }
            return resultSet.getInt(1);
        }
    }

    private void createVersionTableIfNotExists(Connection connection) throws SQLException {
        try (Statement s = connection.createStatement()) {
            // language=MySQL
            s.execute("CREATE TABLE IF NOT EXISTS TXNO_VERSION (table_name VARCHAR(255), version INT)");
        }
    }

    @AllArgsConstructor
    private static final class Migration {
        private final int version;
        private final String name;
        private final String sql;
        private final Map<Dialect, String> dialectSpecific;

        Migration(int version, String name, String sql) {
            this(version, name, sql, Collections.emptyMap());
        }

        String sqlFor(Dialect dialect) {
            return dialectSpecific.getOrDefault(dialect, sql);
        }
    }
}

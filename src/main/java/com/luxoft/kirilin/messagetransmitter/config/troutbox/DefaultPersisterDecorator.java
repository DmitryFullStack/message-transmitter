package com.luxoft.kirilin.messagetransmitter.config.troutbox;

import com.gruelbox.transactionoutbox.*;
import lombok.SneakyThrows;

import java.time.Instant;
import java.util.List;

public class DefaultPersisterDecorator implements Persistor{

    private String tableName;

    @SneakyThrows
    public DefaultPersisterDecorator(Persistor persistor, String tableName) {
        this.persistor = persistor;
        this.tableName = tableName;
    }

    private Persistor persistor;

    @Override
    public void migrate(TransactionManager transactionManager){
        ExtendedMigrationManager.migrate(transactionManager, tableName);
    }


    public void save(Transaction tx, TransactionOutboxEntry entry) throws Exception {
        this.persistor.save(tx, entry);
    }

    public void delete(Transaction tx, TransactionOutboxEntry entry) throws Exception {
        this.persistor.delete(tx, entry);
    }

    public void update(Transaction tx, TransactionOutboxEntry entry) throws Exception {
        this.persistor.update(tx, entry);
    }

    public boolean lock(Transaction tx, TransactionOutboxEntry entry) throws Exception {
        return this.persistor.lock(tx, entry);
    }

    public boolean unblock(Transaction tx, String entryId) throws Exception {
        return this.persistor.unblock(tx, entryId);
    }

    public List<TransactionOutboxEntry> selectBatch(Transaction tx, int batchSize, Instant now) throws Exception {
        return this.persistor.selectBatch(tx, batchSize, now);
    }

    public int deleteProcessedAndExpired(Transaction tx, int batchSize, Instant now) throws Exception {
        return this.persistor.deleteProcessedAndExpired(tx, batchSize, now);
    }
}

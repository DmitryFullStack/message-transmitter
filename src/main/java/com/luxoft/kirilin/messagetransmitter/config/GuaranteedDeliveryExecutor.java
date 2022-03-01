package com.luxoft.kirilin.messagetransmitter.config;

import com.gruelbox.transactionoutbox.TransactionOutbox;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GuaranteedDeliveryExecutor {

    private List<TransactionOutbox> outboxes = new ArrayList<>();

    void addOutbox(TransactionOutbox outbox){
        outboxes.add(outbox);
    }

    Thread backgroundThread = new Thread(() -> {
        log.info("Start listening transactional outbox tables...");
        while (!Thread.interrupted()) {
            try {
                for (TransactionOutbox outbox : outboxes) {
                    // Keep flushing work until there's nothing left to flush
                    while (outbox.flush()) {
                    }
                }

            } catch (Exception e) {
                log.error("Error flushing transaction outbox. Pausing", e);
            }
            try {
                // When we run out of work, pause for a minute before checking again
                Thread.sleep(30_000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }, "transactional-outbox_executor_thread");

}

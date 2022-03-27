package com.luxoft.kirilin.messagetransmitter.config.troutbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Objects.isNull;

@Slf4j
public class GuaranteedDeliveryExecutorService {

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private DataSource dataSource;
    private final Map<String, GuaranteedDelivery> outboxes = new HashMap<>();
    private ObjectMapper objectMapper;

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void addForGuaranteedDelivery(Object msg, String tableName) {
        String fullTableName = outboxes.get(tableName).fullTableName;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(String.format("select * from %s limit 1000", fullTableName) )) {
            connection.setAutoCommit(false);
            List<GuaranteedDeliveryEntity> guaranteedDeliveries = new ArrayList<>();
            ps.setString(1, fullTableName);
            try (ResultSet resultSet = ps.executeQuery()) {
                while (resultSet.next()) {
                    GuaranteedDeliveryEntity deliveryEntity = new GuaranteedDeliveryEntity();
                    deliveryEntity.setId(resultSet.getObject("id", UUID.class));
                    deliveryEntity.setPayload(resultSet.getString("payload"));
//                        deliveryEntity.setPartition(resultSet.getInt("partition"));
                    deliveryEntity.setApproved(resultSet.getBoolean("approved"));
//                        deliveryEntity.setCreateTime(resultSet.getObject("createTime", LocalDateTime.class));
                    guaranteedDeliveries.add(deliveryEntity);
                }
            }
            connection.commit();
            if(guaranteedDeliveries.isEmpty()){
                try {
                    outboxes.get(fullTableName).messageSender.accept(objectMapper.writeValueAsString(msg));
                    return;
                } catch (Exception exception){
                    log.error("Problem detected during sending msg:\n" + exception.getMessage());
                }
            }
            try (PreparedStatement insertPS = connection.prepareStatement(String.format("insert into %s (id, payload, partition, create_time) values (?, ?, ?, now())", fullTableName))) {
                insertPS.setObject(1, UUID.randomUUID());
                insertPS.setString(2, fullTableName);
                insertPS.setString(3, objectMapper.writeValueAsString(msg));
                insertPS.setInt(4, 0);
                int i = insertPS.executeUpdate();
                if(i != 1){
                    throw new SQLException("Could not guaranteed delivery, because of fault while persist!");
                }
            }
            connection.commit();
        }catch (SQLException | JsonProcessingException ex){
            log.error(ex.getMessage());
            System.exit(1);
        }
    }

    public void addOutbox(ApplicationContext context,
                          String tableName, Consumer<String> sender) {
        synchronized (GuaranteedDeliveryExecutorService.class) {
            if (isNull(dataSource)) {
                dataSource = context.getBean(DataSource.class);
            }
        }
        String tableFullName = tableName + "_TXOUTBOX";
        ExtendedMigrationManager.migrate(dataSource, tableFullName);
        GuaranteedDelivery guaranteedDelivery = new GuaranteedDelivery();
        guaranteedDelivery.setMessageSender(sender);
        guaranteedDelivery.setFullTableName(tableFullName);
        outboxes.put(tableName, guaranteedDelivery);
        publishOutboxData(tableFullName);
    }

    private void publishOutboxData(String tableFullName) {
        GuaranteedDelivery guaranteedDelivery = outboxes.get(tableFullName);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            log.info(tableFullName + " table handling...");
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement ps = connection.prepareStatement(String.format("select * from %s where approved = false order by create_time asc limit 1000", tableFullName))) {
                List<GuaranteedDeliveryEntity> guaranteedDeliveries = new ArrayList<>();
                connection.setAutoCommit(false);
                try (ResultSet resultSet = ps.executeQuery()) {
                    while (resultSet.next()) {
                        GuaranteedDeliveryEntity deliveryEntity = new GuaranteedDeliveryEntity();
                        deliveryEntity.setId(resultSet.getObject("id", UUID.class));
                        deliveryEntity.setPayload(resultSet.getString("payload"));
//                        deliveryEntity.setPartition(resultSet.getInt("partition"));
                        deliveryEntity.setApproved(resultSet.getBoolean("approved"));
//                        deliveryEntity.setCreateTime(resultSet.getObject("createTime", LocalDateTime.class));
                        guaranteedDeliveries.add(deliveryEntity);
                    }
                }
                connection.commit();
                log.info(guaranteedDeliveries.size() + " records found for sending to the recipient...");
                for (GuaranteedDeliveryEntity entity : guaranteedDeliveries) {
                    try {
                        guaranteedDelivery.messageSender.accept(entity.payload);
                    } catch (Exception e) {
                        break;
                    }
                    try (PreparedStatement updatePS = connection.prepareStatement(String.format("update %s set approved = true where id = ?", tableFullName))) {
                        updatePS.setString(1, tableFullName);
                        updatePS.setObject(2, entity.id);
                        int i = updatePS.executeUpdate();
                        if(i != 1){
                            break;
                        }
                    }
                    connection.commit();
                }
                log.info(guaranteedDeliveries.size() + " records was successful sent!");
            }catch (SQLException exception){
                log.error("Error while guaranteed delivery algorithm:\n" + exception.getMessage());
            }
        }, 10, 20, TimeUnit.SECONDS);
    }


//        public Thread backgroundThread = new Thread(() -> {
//            log.info("Start listening transactional outbox tables...");
//            while (!Thread.interrupted()) {
//                try {
//                    for (TransactionOutbox outbox : outboxes) {
//                        // Keep flushing work until there's nothing left to flush
//                        while (outbox.flush()) {
//                        }
//                    }
//
//                } catch (Exception e) {
//                    log.error("Error flushing transaction outbox. Pausing", e);
//                }
//                try {
//                    // When we run out of work, pause for 30 seconds before checking again
//                    Thread.sleep(30_000);
//                } catch (InterruptedException e) {
//                    break;
//                }
//            }
//        }, "transactional-outbox_executor_thread");

    static class GuaranteedDelivery {
        private int[] partitions;
        private Consumer<String> messageSender;
        private String fullTableName;

        public String getFullTableName() {
            return fullTableName;
        }

        public void setFullTableName(String fullTableName) {
            this.fullTableName = fullTableName;
        }

        public int[] getPartitions() {
            return partitions;
        }

        public void setPartitions(int[] partitions) {
            this.partitions = partitions;
        }

        public Consumer<String> getMessageSender() {
            return messageSender;
        }

        public void setMessageSender(Consumer<String> messageSender) {
            this.messageSender = messageSender;
        }
    }

    static class GuaranteedDeliveryEntity {
        private UUID id;
        private String payload;
        private boolean approved;
        private int partition;
        private LocalDateTime createTime;

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }

        public boolean isApproved() {
            return approved;
        }

        public void setApproved(boolean approved) {
            this.approved = approved;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public LocalDateTime getCreateTime() {
            return createTime;
        }

        public void setCreateTime(LocalDateTime createTime) {
            this.createTime = createTime;
        }
    }
}

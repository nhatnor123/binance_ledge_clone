package com.ledger.clone.raft;

import com.ledger.clone.common.proto.TransactionRequest;
import com.ledger.clone.raft.config.RaftConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class LearnerNode extends BaseStateMachine {

    private static final Logger log = LoggerFactory.getLogger(LearnerNode.class);

    private static final String KAFKA_TOPIC = "ledger-balance-changes";

    private final RaftConfig config;
    private final KafkaProducer<String, String> producer;
    private final Object lock = new Object();
    private volatile boolean running;

    public LearnerNode(RaftConfig config) {
        this.config = config;
        this.producer = createKafkaProducer();
    }

    @Override
    public void initialize(RaftServer server, org.apache.ratis.protocol.RaftGroupId groupId,
                           RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        running = true;
        log.info("Learner node {} initialized, publishing to Kafka topic '{}'",
                config.getNodeId(), KAFKA_TOPIC);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        var logEntry = trx.getLogEntry();
        var logData = logEntry.getStateMachineLogEntry().getLogData();

        TransactionRequest proto;
        try {
            proto = TransactionRequest.parseFrom(logData.toByteArray());
        } catch (Exception e) {
            return CompletableFuture.completedFuture(
                    Message.valueOf("ERROR: Invalid log data"));
        }

        try {
            var json = toJsonString(proto, logEntry.getIndex());
            var record = new ProducerRecord<>(KAFKA_TOPIC, proto.getRequestId(), json);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to publish to Kafka: {}", exception.getMessage());
                }
            });
        } catch (Exception e) {
            log.error("Error publishing to Kafka: {}", e.getMessage());
        }

        synchronized (lock) {
            updateLastAppliedTermIndex(logEntry.getTerm(), logEntry.getIndex());
        }

        return CompletableFuture.completedFuture(
                Message.valueOf("OK"));
    }

    private String toJsonString(TransactionRequest proto, long logIndex) {
        return "{" +
                "\"txId\":\"" + proto.getRequestId() + "\"," +
                "\"type\":\"" + proto.getType() + "\"," +
                "\"fromUserId\":\"" + proto.getFromUserId() + "\"," +
                "\"toUserId\":\"" + proto.getToUserId() + "\"," +
                "\"asset\":\"" + proto.getAsset() + "\"," +
                "\"amount\":\"" + proto.getAmount() + "\"," +
                "\"raftLogIndex\":" + logIndex +
                "}";
    }

    public boolean isRunning() {
        return running;
    }

    public void shutdown() {
        running = false;
        producer.flush();
        producer.close();
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return new KafkaProducer<>(props);
    }
}

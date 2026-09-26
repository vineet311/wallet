package com.walletledger.ledger;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * What happens when handling a message fails.
 *
 * Bad message (InvalidEventException): sent to <topic>.dlq at once, and the consumer moves on.
 *
 * Anything else (for example PostgreSQL is down): retried forever with backoff
 * (0.5s, 1s, 2s ... capped at 10s). The offset is not committed, so nothing is lost,
 * and the consumer carries on from the same message once the database is back.
 * Sending it to the DLQ instead would lose a real transfer from the ledger.
 */
@Configuration
class KafkaErrorConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> template) {
        // Partition -1 lets Kafka pick; the .dlq topics have a single partition.
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + ".dlq", -1));

        var backoff = new ExponentialBackOff(500, 2.0);
        backoff.setMaxInterval(10_000);

        var handler = new DefaultErrorHandler(recoverer, backoff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }
}
package com.walletledger.ledger;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Reads transfer and deposit events and turns each into balanced journal lines.
 *
 *   Transfer A -> B:  DEBIT wallet:A, CREDIT wallet:B
 *   Deposit to A:     DEBIT external:cash, CREDIT wallet:A
 *
 * A wallet account's balance is credits minus debits.
 *
 * The offset is committed only after this method returns, so delivery is at-least-once.
 * JournalService skips events it has already recorded.
 */
@Component
public class LedgerListener {

    static final String TRANSFERS = "transfers.completed";
    static final String DEPOSITS = "wallet.deposited";
    private static final String EXTERNAL_CASH = "external:cash";

    private static final Logger log = LoggerFactory.getLogger(LedgerListener.class);

    private final JournalService journal;
    private final JsonMapper json;

    public LedgerListener(JournalService journal, JsonMapper json) {
        this.journal = journal;
        this.json = json;
    }

    @KafkaListener(
            topics = {TRANSFERS, DEPOSITS},
            groupId = "ledger",
            concurrency = "${LEDGER_CONCURRENCY:3}")
    public void onEvent(ConsumerRecord<String, String> record) {
        JsonNode event = parse(record.value());
        String eventId = require(event, "eventId");
        String type = require(event, "type");
        String transactionId = require(event, "transactionId");
        List<JournalService.Line> lines = linesFor(record.topic(), event);

        boolean posted = journal.post(eventId, type, transactionId, lines);
        if (posted) {
            log.info("journal entry posted eventId={} type={} partition={} offset={}",
                    eventId, type, record.partition(), record.offset());
        } else {
            log.info("duplicate event skipped eventId={}", eventId);
        }
    }

    private List<JournalService.Line> linesFor(String topic, JsonNode event) {
        long amount = amount(event);
        return switch (topic) {
            case TRANSFERS -> List.of(
                    new JournalService.Line("wallet:" + require(event, "fromWalletId"), "DEBIT", amount),
                    new JournalService.Line("wallet:" + require(event, "toWalletId"), "CREDIT", amount));
            case DEPOSITS -> List.of(
                    new JournalService.Line(EXTERNAL_CASH, "DEBIT", amount),
                    new JournalService.Line("wallet:" + require(event, "walletId"), "CREDIT", amount));
            default -> throw new InvalidEventException("unexpected topic " + topic);
        };
    }

    private JsonNode parse(String value) {
        if (value == null) {
            throw new InvalidEventException("message has no value");
        }
        try {
            JsonNode node = json.readTree(value);
            if (node == null || !node.isObject()) {
                throw new InvalidEventException("message is not a JSON object");
            }
            return node;
        } catch (JacksonException e) {
            throw new InvalidEventException("message is not valid JSON");
        }
    }

    private static String require(JsonNode event, String field) {
        JsonNode v = event.get(field);
        if (v == null || !v.isString() || v.stringValue().isBlank()) {
            throw new InvalidEventException("event is missing field \"" + field + "\"");
        }
        return v.stringValue();
    }

    private static long amount(JsonNode event) {
        JsonNode v = event.get("amountCents");
        if (v == null || !v.isIntegralNumber() || v.asLong() <= 0) {
            throw new InvalidEventException("amountCents must be a positive integer");
        }
        return v.asLong();
    }
}
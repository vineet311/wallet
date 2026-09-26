package com.walletledger.ledger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Writes journal entries. One entry per event, two or more balanced lines per entry. */
@Service
public class JournalService {

    public record Line(String account, String direction, long amountCents) {}

    private final JdbcClient jdbc;
    private final MeterRegistry registry;
    private final Counter duplicates;

    public JournalService(JdbcClient jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        this.registry = registry;
        this.duplicates = Counter.builder("ledger.duplicate.events")
                .description("Events skipped because they were already recorded")
                .register(registry);
    }

    /**
     * Records one event. Returns false if this event was already recorded.
     *
     * Idempotency: event_id is UNIQUE, so a redelivered event inserts nothing
     * and we skip it. The database trigger checks debits = credits at COMMIT.
     */
    @Transactional
    public boolean post(String eventId, String eventType, String referenceId, List<Line> lines) {
        Optional<UUID> entryId = jdbc.sql("""
                INSERT INTO ledger.journal_entries (event_id, event_type, reference_id)
                VALUES (?, ?, ?::uuid)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
                """)
                .params(eventId, eventType, referenceId)
                .query(UUID.class)
                .optional();

        if (entryId.isEmpty()) {
            duplicates.increment();
            return false;
        }
        for (Line line : lines) {
            jdbc.sql("""
                    INSERT INTO ledger.journal_lines (entry_id, account, direction, amount_cents)
                    VALUES (?, ?, ?, ?)
                    """)
                    .params(entryId.get(), line.account(), line.direction(), line.amountCents())
                    .update();
        }
        Counter.builder("ledger.entries.posted")
                .description("Journal entries written")
                .tag("event_type", eventType)
                .register(registry)
                .increment();
        return true;
    }
}
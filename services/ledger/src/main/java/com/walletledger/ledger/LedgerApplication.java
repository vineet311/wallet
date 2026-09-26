package com.walletledger.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The ledger service. It reads money events from Kafka and records each one
 * as a balanced double-entry journal entry in PostgreSQL. It has no public API:
 * its only HTTP endpoints are /healthz, /readyz and /metrics on OPS_PORT.
 */
@SpringBootApplication
public class LedgerApplication {
    public static void main(String[] args) {
        SpringApplication.run(LedgerApplication.class, args);
    }
}
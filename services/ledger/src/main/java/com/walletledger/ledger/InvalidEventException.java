package com.walletledger.ledger;

/**
 * The event itself is broken (not JSON, missing a field, bad amount).
 * Retrying can never fix it, so the error handler sends it straight to the dead-letter topic.
 */
public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message) {
        super(message);
    }
}
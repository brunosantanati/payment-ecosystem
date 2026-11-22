package com.bruno.santana.payout_service.exception;

public class NonRetryablePayoutException extends RuntimeException {
    public NonRetryablePayoutException(String message) {
        super(message);
    }
}
package com.bruno.santana.payout_service.gateway;

public record P2aStatusUpdateRequest(
        String payoutId,
        String status,
        String reason
) {
}
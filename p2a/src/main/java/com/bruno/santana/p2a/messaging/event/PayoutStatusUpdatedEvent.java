package com.bruno.santana.p2a.messaging.event;

import com.bruno.santana.p2a.domain.PayoutStatus;
import java.time.Instant;
import java.util.UUID;

public record PayoutStatusUpdatedEvent(
        UUID eventId,
        String payoutId,
        PayoutStatus status,
        String reason,
        Instant occurredAt
) {
    public static PayoutStatusUpdatedEvent of(String payoutId, PayoutStatus status, String reason) {
        return new PayoutStatusUpdatedEvent(
                UUID.randomUUID(),
                payoutId,
                status,
                reason,
                Instant.now()
        );
    }
}

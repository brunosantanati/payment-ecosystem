package com.bruno.santana.p2a.api.dto;

import com.bruno.santana.p2a.domain.PayoutStatus;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record P2aWebhookRequest(
        @NotBlank String payoutId,
        @NotNull PayoutStatus status,
        String reason
) {}

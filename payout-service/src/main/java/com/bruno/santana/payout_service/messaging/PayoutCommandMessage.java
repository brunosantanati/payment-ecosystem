package com.bruno.santana.payout_service.messaging;

import java.math.BigDecimal;

public record PayoutCommandMessage(
        String payoutId,
        String correlationId,
        String apm,           // Ex: "WERO"
        BigDecimal amount,
        String currency,      // Ex: "EUR"
        String beneficiaryIban
) {}

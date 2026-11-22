package com.bruno.santana.payout_service.gateway;

import com.bruno.santana.payout_service.messaging.PayoutCommandMessage;

public interface PayoutGatewayClient {
    void sendToP2a(PayoutCommandMessage command);
}
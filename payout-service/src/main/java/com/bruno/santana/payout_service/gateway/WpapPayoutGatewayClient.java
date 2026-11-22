package com.bruno.santana.payout_service.gateway;

import com.bruno.santana.payout_service.exception.NonRetryablePayoutException;
import com.bruno.santana.payout_service.messaging.PayoutCommandMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class WpapPayoutGatewayClient implements PayoutGatewayClient {

    private static final Logger log = LoggerFactory.getLogger(WpapPayoutGatewayClient.class);

    private final RestClient p2aRestClient;

    public WpapPayoutGatewayClient(RestClient p2aRestClient) {
        this.p2aRestClient = p2aRestClient;
    }

    // Ponto único de integração externa: fácil de anotar com @CircuitBreaker e @Retry (Resilience4j) no futuro
    @Override
    public void sendToP2a(PayoutCommandMessage command) {
        P2aStatusUpdateRequest requestBody = new P2aStatusUpdateRequest(
                command.payoutId(),
                "PAYOUT_APPROVED",
                null
        );

        log.info("Chamando P2A POST /webhooks/p2a/status-updates | payoutId={} | correlationId={}",
                command.payoutId(), command.correlationId());

        p2aRestClient.post()
                .uri("/webhooks/p2a/status-updates")
                .header("X-Correlation-Id", command.correlationId())
                .body(requestBody)
                .retrieve()
                // Erros 4xx (ex: 400 Bad Request de validação no @Valid do P2aWebhookController) não adianta retentar
                .onStatus(HttpStatusCode::is4xxClientError, (req, res) -> {
                    throw new NonRetryablePayoutException(
                            "P2A rejeitou a requisição com HTTP " + res.getStatusCode().value()
                    );
                })
                // Erros 5xx lançam RestClientResponseException (RuntimeException) -> SQS faz retry com backoff/visibility
                .toBodilessEntity();

        log.info("P2A respondeu com sucesso (202 Accepted) | payoutId={}", command.payoutId());
    }
}
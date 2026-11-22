package com.bruno.santana.payout_service.service;

import com.bruno.santana.payout_service.domain.PayoutRequest;
import com.bruno.santana.payout_service.domain.PayoutStatus;
import com.bruno.santana.payout_service.gateway.PayoutGatewayClient;
import com.bruno.santana.payout_service.messaging.PayoutCommandMessage;
import com.bruno.santana.payout_service.repository.PayoutRequestRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

@Service
public class PayoutProcessingService {

    private static final Logger log = LoggerFactory.getLogger(PayoutProcessingService.class);

    private final PayoutRequestRepository repository;
    private final PayoutGatewayClient gatewayClient;
    private final TransactionTemplate transactionTemplate;
    private final JsonMapper jsonMapper;

    public PayoutProcessingService(
            PayoutRequestRepository repository,
            PayoutGatewayClient gatewayClient,
            PlatformTransactionManager transactionManager,
            JsonMapper jsonMapper) {
        this.repository = repository;
        this.gatewayClient = gatewayClient;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.jsonMapper = jsonMapper;
    }

    public void process(PayoutCommandMessage command) {
        String payoutId = command.payoutId();

        if (repository.existsById(payoutId)) {
            log.info("Payout já registrado anteriormente (idempotência). Ignorando reprocessamento | payoutId={}", payoutId);
            return;
        }

        gatewayClient.sendToP2a(command);

        saveIdempotently(command);
    }

    private void saveIdempotently(PayoutCommandMessage command) {
        String payloadJson = jsonMapper.writeValueAsString(command);

        try {
            transactionTemplate.executeWithoutResult(status -> {
                PayoutRequest entity = new PayoutRequest(
                        command.payoutId(),
                        payloadJson,
                        PayoutStatus.PAYOUT_SENT_TO_P2A
                );
                repository.saveAndFlush(entity);
            });
            log.info("Payout persistido no PostgreSQL | payoutId={} | status={}",
                    command.payoutId(), PayoutStatus.PAYOUT_SENT_TO_P2A);
        } catch (DataIntegrityViolationException ex) {
            log.warn("Concorrência detectada ao persistir payout_id={} (já existe na base). Tratando como idempotente.",
                    command.payoutId());
        }
    }
}
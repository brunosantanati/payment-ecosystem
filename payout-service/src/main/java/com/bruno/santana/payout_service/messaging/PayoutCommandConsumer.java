package com.bruno.santana.payout_service.messaging;

import com.bruno.santana.payout_service.exception.NonRetryablePayoutException;
import com.bruno.santana.payout_service.service.PayoutProcessingService;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PayoutCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(PayoutCommandConsumer.class);

    private final PayoutProcessingService processingService;
    private final DlqPublisher dlqPublisher;

    public PayoutCommandConsumer(
            PayoutProcessingService processingService,
            DlqPublisher dlqPublisher) {
        this.processingService = processingService;
        this.dlqPublisher = dlqPublisher;
    }

    @SqsListener(
            value = "${app.sqs.payout-commands-queue}",
            acknowledgementMode = "MANUAL",
            maxConcurrentMessages = "5",
            maxMessagesPerPoll = "5"
    )
    public void onPayoutCommand(
            @Payload PayoutCommandMessage command,
            @Header(name = "ApproximateReceiveCount", defaultValue = "1") String receiveCount,
            @Header(name = "id", required = false) String sqsMessageId,
            Acknowledgement ack) {

        log.info("Recebido comando de payout | sqsMessageId={} | receiveCount={} | payoutId={} | correlationId={}",
                sqsMessageId, receiveCount, command.payoutId(), command.correlationId());

        try {
            if (isInvalidContract(command)) {
                log.error("Payload invalido (Poison Pill). Encaminhando direto para DLQ | sqsMessageId={}", sqsMessageId);
                dlqPublisher.publish(command, "INVALID_PAYLOAD");
                ack.acknowledge();
                return;
            }

            processingService.process(command);

            ack.acknowledge();
            log.info("Comando processado e removido da fila | payoutId={}", command.payoutId());

        } catch (NonRetryablePayoutException ex) {
            log.error("Erro definitivo no payout. Movendo para DLQ | payoutId={} | motivo={}",
                    command.payoutId(), ex.getMessage());
            dlqPublisher.publish(command, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            log.warn("Falha transiente no processamento (tentativa {}/3). Mensagem retornara apos visibility timeout | payoutId={} | erro={}",
                    receiveCount, command.payoutId(), ex.getMessage());
            throw ex;
        }
    }

    private boolean isInvalidContract(PayoutCommandMessage command) {
        return command == null
                || command.payoutId() == null
                || command.payoutId().isBlank()
                || command.amount() == null;
    }
}
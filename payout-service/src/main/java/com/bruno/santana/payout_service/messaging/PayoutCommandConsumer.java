package com.bruno.santana.payout_service.messaging;

import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class PayoutCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(PayoutCommandConsumer.class);

    private final SqsTemplate sqsTemplate;
    private final String dlqName;

    public PayoutCommandConsumer(
            SqsTemplate sqsTemplate,
            @Value("${app.sqs.payout-commands-dlq}") String dlqName) {
        this.sqsTemplate = sqsTemplate;
        this.dlqName = dlqName;
    }

    /**
     * AcknowledgementMode.MANUAL garante controle exato:
     * - Sucesso ou duplicata ja processada -> ack.acknowledge() (deleta da fila).
     * - Falha transiente (timeout WPAP/DB) -> nao faz ack; apos visibilityTimeout (60s)
     *   o SQS reentrega ate 3x e move sozinho para a DLQ.
     * - Erro irrecuperavel (payload invalido) -> encaminha imediatamente para a DLQ e da ack.
     */
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
            // 1. Validacao basica de contrato (Poison Pill check)
            if (command.payoutId() == null || command.payoutId().isBlank() || command.amount() == null) {
                log.error("Payload invalido (Poison Pill). Encaminhando direto para DLQ | sqsMessageId={}", sqsMessageId);
                sendDirectlyToDlq(command, "INVALID_PAYLOAD");
                ack.acknowledge();
                return;
            }

            // 2. Processamento idempotente:
            //    - Verifica latest_state no Aurora PostgreSQL
            //    - Grava estado inicial / audit_log
            //    - Chama WPAP P2A API via thin library
            //    - Atualiza latest_state e publica evento no Kafka
            processPayout(command);

            // 3. Remove mensagem da fila principal somente apos commit/confirmacao
            ack.acknowledge();
            log.info("Comando processado e removido da fila | payoutId={}", command.payoutId());

        } catch (NonRetryablePayoutException ex) {
            // Erros de negocio definitivos (ex: conta encerrada, moeda nao suportada)
            // Nao adianta tentar 3x; envia direto para DLQ e remove da fila principal
            log.error("Erro definitivo no payout. Movendo para DLQ | payoutId={} | motivo={}",
                    command.payoutId(), ex.getMessage());
            sendDirectlyToDlq(command, ex.getMessage());
            ack.acknowledge();

        } catch (Exception ex) {
            // Falha transiente (ex: Timeout WPAP, HTTP 503, falha de conexao DB)
            // NAO chama ack.acknowledge(): o SQS vai reentregar ate maxReceiveCount=3
            // e mover automaticamente para a DLQ disparamdo o alarme do CloudWatch.
            log.warn("Falha transiente no processamento (tentativa {}/3). Mensagem retornara apos visibility timeout | payoutId={} | erro={}",
                    receiveCount, command.payoutId(), ex.getMessage());
            throw ex;
        }
    }

    private void sendDirectlyToDlq(PayoutCommandMessage command, String errorReason) {
        sqsTemplate.send(to -> to
                .queue(dlqName)
                .payload(command)
                .header("dlq-reason", errorReason)
        );
    }

    private void processPayout(PayoutCommandMessage command) {
        // TODO: Integrar Service de maquina de estados (Aurora) + Client WPAP + Producer Kafka
        log.info("Executando orquestracao de payout para payoutId={} valor={} {}",
                command.payoutId(), command.amount(), command.currency());
    }

    public static class NonRetryablePayoutException extends RuntimeException {
        public NonRetryablePayoutException(String message) {
            super(message);
        }
    }
}

package com.bruno.santana.p2a.messaging;

import com.bruno.santana.p2a.messaging.event.PayoutStatusUpdatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PayoutStatusProducer {

    private static final Logger log = LoggerFactory.getLogger(PayoutStatusProducer.class);

    private final KafkaTemplate<String, PayoutStatusUpdatedEvent> kafkaTemplate;
    private final String topicName;

    public PayoutStatusProducer(
            KafkaTemplate<String, PayoutStatusUpdatedEvent> kafkaTemplate,
            @Value("${app.kafka.topics.payout-status}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void publish(PayoutStatusUpdatedEvent event) {
        kafkaTemplate.send(topicName, event.payoutId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Falha ao publicar evento {} para payoutId={}", event.eventId(), event.payoutId(), ex);
                    } else {
                        log.info("Evento publicado | topic={} | partition={} | offset={} | payoutId={} | status={}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                event.payoutId(),
                                event.status());
                    }
                });
    }
}
package com.bruno.santana.p2a.api;

import com.bruno.santana.p2a.api.dto.P2aWebhookRequest;
import com.bruno.santana.p2a.messaging.event.PayoutStatusUpdatedEvent;
import com.bruno.santana.p2a.messaging.PayoutStatusProducer;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/webhooks/p2a")
public class P2aWebhookController {

    private final PayoutStatusProducer producer;

    public P2aWebhookController(PayoutStatusProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/status-updates")
    public ResponseEntity<Void> receiveStatusUpdate(@Valid @RequestBody P2aWebhookRequest request) {
        var event = PayoutStatusUpdatedEvent.of(
                request.payoutId(),
                request.status(),
                request.reason()
        );

        producer.publish(event);

        return ResponseEntity.accepted().build();
    }
}
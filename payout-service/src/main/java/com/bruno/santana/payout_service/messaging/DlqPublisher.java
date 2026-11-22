package com.bruno.santana.payout_service.messaging;

import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class DlqPublisher {

    private final SqsTemplate sqsTemplate;
    private final String dlqName;

    public DlqPublisher(
            SqsTemplate sqsTemplate,
            @Value("${app.sqs.payout-commands-dlq}") String dlqName) {
        this.sqsTemplate = sqsTemplate;
        this.dlqName = dlqName;
    }

    public void publish(PayoutCommandMessage command, String errorReason) {
        sqsTemplate.send(to -> to
                .queue(dlqName)
                .payload(command)
                .header("dlq-reason", errorReason)
        );
    }
}
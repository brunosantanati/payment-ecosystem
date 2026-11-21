package com.bruno.santana.payout_service.messaging;

import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@Testcontainers
class PayoutCommandConsumerIntegrationTest {

    private static final String MAIN_QUEUE_NAME = "payment-ecosystem-payout-commands";
    private static final String DLQ_NAME = "payment-ecosystem-payout-commands-dlq";
    private static final String DEFAULT_IBAN = "DE89370400440532013000";

    // ElasticMQ: 100% open-source (Apache 2.0), leve e sem restrições comerciais
    @Container
    static GenericContainer<?> sqsContainer = new GenericContainer<>(
            DockerImageName.parse("softwaremill/elasticmq-native:latest"))
            .withExposedPorts(9324);

    private static String mainQueueUrl;
    private static String dlqUrl;

    @Autowired
    private SqsTemplate sqsTemplate;

    @Autowired
    private SqsAsyncClient sqsAsyncClient;

    @MockitoSpyBean
    private PayoutCommandConsumer consumerSpy;

    @DynamicPropertySource
    static void registerSqsProperties(DynamicPropertyRegistry registry) {
        String endpoint = "http://" + sqsContainer.getHost() + ":" + sqsContainer.getMappedPort(9324);

        registry.add("spring.cloud.aws.sqs.endpoint", () -> endpoint);
        registry.add("spring.cloud.aws.region.static", () -> "us-east-1");
        registry.add("spring.cloud.aws.credentials.access-key", () -> "test");
        registry.add("spring.cloud.aws.credentials.secret-key", () -> "test");

        registry.add("app.sqs.payout-commands-queue", () -> MAIN_QUEUE_NAME);
        registry.add("app.sqs.payout-commands-dlq", () -> DLQ_NAME);
    }

    /**
     * Cria as filas ANTES do contexto do Spring Boot subir, garantindo que o @SqsListener
     * encontre a fila principal imediatamente ao inicializar.
     */
    @BeforeAll
    static void createQueuesBeforeSpringContext() throws Exception {
        String endpoint = "http://" + sqsContainer.getHost() + ":" + sqsContainer.getMappedPort(9324);

        try (SqsAsyncClient setupClient = SqsAsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build()) {

            // 1. Cria a DLQ
            dlqUrl = setupClient.createQueue(CreateQueueRequest.builder()
                    .queueName(DLQ_NAME)
                    .build()).get().queueUrl();

            String dlqArn = setupClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(dlqUrl)
                    .attributeNames(QueueAttributeName.QUEUE_ARN)
                    .build()).get().attributes().get(QueueAttributeName.QUEUE_ARN);

            // 2. Cria a fila principal com VisibilityTimeout de 2s (para o teste de 3 retentativas ser rápido)
            // e RedrivePolicy nativa do SQS para mover à DLQ após maxReceiveCount = 3
            String redrivePolicy = """
                    {"deadLetterTargetArn":"%s","maxReceiveCount":"3"}
                    """.formatted(dlqArn);

            mainQueueUrl = setupClient.createQueue(CreateQueueRequest.builder()
                    .queueName(MAIN_QUEUE_NAME)
                    .attributes(Map.of(
                            QueueAttributeName.VISIBILITY_TIMEOUT, "2",
                            QueueAttributeName.REDRIVE_POLICY, redrivePolicy
                    ))
                    .build()).get().queueUrl();
        }
    }

    @BeforeEach
    void purgeQueues() throws Exception {
        sqsAsyncClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(mainQueueUrl).build()).get();
        sqsAsyncClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(dlqUrl).build()).get();
    }

    @Test
    void deveProcessarComandoValidoEDeletarDaFilaPrincipal() throws Exception {
        String payoutId = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();
        PayoutCommandMessage command = new PayoutCommandMessage(
                payoutId,
                correlationId,
                "WERO",
                new BigDecimal("250.00"),
                "EUR",
                DEFAULT_IBAN
        );

        sqsTemplate.send(MAIN_QUEUE_NAME, command);

        // Aguarda a execução do método processPayout
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                verify(consumerSpy, times(1)).processPayout(command)
        );

        // Garante que ack.acknowledge() removeu a mensagem da fila principal e nada foi para a DLQ
        Thread.sleep(2500); // Aguarda passar o visibility timeout de 2s para ter certeza que não reaparece
        assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
        assertThat(receiveMessagesFrom(dlqUrl)).isEmpty();
    }

    @Test
    void deveEncaminharPoisonPillDiretoParaDlqSemRetentar() {
        // Payload inválido (payoutId em branco e amount nulo)
        PayoutCommandMessage invalidCommand = new PayoutCommandMessage(
                "",
                UUID.randomUUID().toString(),
                "WERO",
                null,
                "EUR",
                DEFAULT_IBAN
        );

        sqsTemplate.send(MAIN_QUEUE_NAME, invalidCommand);

        // Deve ir imediatamente para a DLQ com o header dlq-reason = INVALID_PAYLOAD
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var dlqMessages = receiveMessagesFrom(dlqUrl);
            assertThat(dlqMessages).hasSize(1);
            assertThat(dlqMessages.get(0).messageAttributes().get("dlq-reason").stringValue())
                    .isEqualTo("INVALID_PAYLOAD");
        });

        // Nunca deve tentar executar a orquestração nem ficar retentando na fila principal
        verify(consumerSpy, never()).processPayout(any());
        assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
    }

    @Test
    void deveMoverImediatamenteParaDlqQuandoOcorrerNonRetryablePayoutException() {
        String payoutId = UUID.randomUUID().toString();
        PayoutCommandMessage command = new PayoutCommandMessage(
                payoutId,
                UUID.randomUUID().toString(),
                "WERO",
                new BigDecimal("100.00"),
                "EUR",
                DEFAULT_IBAN
        );

        doThrow(new PayoutCommandConsumer.NonRetryablePayoutException("ACCOUNT_CLOSED"))
                .when(consumerSpy).processPayout(command);

        sqsTemplate.send(MAIN_QUEUE_NAME, command);

        // Deve ir direto para a DLQ na 1ª tentativa com o motivo da exceção
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var dlqMessages = receiveMessagesFrom(dlqUrl);
            assertThat(dlqMessages).hasSize(1);
            assertThat(dlqMessages.get(0).body()).contains(payoutId);
            assertThat(dlqMessages.get(0).messageAttributes().get("dlq-reason").stringValue())
                    .isEqualTo("ACCOUNT_CLOSED");
        });

        // Garante que foi executado apenas 1 vez (sem retentativas)
        verify(consumerSpy, times(1)).processPayout(command);
        assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
    }

    @Test
    void deveRetentarTresVezesEMoverParaDlqQuandoOcorrerFalhaTransiente() {
        String payoutId = UUID.randomUUID().toString();
        PayoutCommandMessage command = new PayoutCommandMessage(
                payoutId,
                UUID.randomUUID().toString(),
                "WERO",
                new BigDecimal("999.90"),
                "EUR",
                DEFAULT_IBAN
        );

        // Simula timeout no WPAP / falha de rede (exceção genérica)
        doThrow(new RuntimeException("WPAP ReadTimeoutException"))
                .when(consumerSpy).processPayout(command);

        sqsTemplate.send(MAIN_QUEUE_NAME, command);

        // Como maxReceiveCount = 3 e VisibilityTimeout = 2s, após ~4-6s o SQS move para a DLQ
        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var dlqMessages = receiveMessagesFrom(dlqUrl);
            assertThat(dlqMessages).hasSize(1);
            assertThat(dlqMessages.get(0).body()).contains(payoutId);
        });

        // Confirma que o consumidor tentou exatamente 3 vezes antes do SQS mover para a DLQ
        verify(consumerSpy, times(3)).processPayout(command);
        assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
    }

    @Test
    void deveRecuperarComSucessoNaSegundaTentativaAposFalhaTransienteInicial() {
        String payoutId = UUID.randomUUID().toString();
        PayoutCommandMessage command = new PayoutCommandMessage(
                payoutId,
                UUID.randomUUID().toString(),
                "WERO",
                new BigDecimal("420.00"),
                "EUR",
                DEFAULT_IBAN
        );

        // 1ª tentativa falha (HTTP 503), 2ª tentativa funciona
        doThrow(new RuntimeException("HTTP 503 Service Unavailable"))
                .doNothing()
                .when(consumerSpy).processPayout(command);

        sqsTemplate.send(MAIN_QUEUE_NAME, command);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                verify(consumerSpy, times(2)).processPayout(command)
        );

        // Garante que a mensagem foi confirmada na 2ª tentativa e não foi para a DLQ
        assertThat(receiveMessagesFrom(dlqUrl)).isEmpty();
        assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
    }

    private java.util.List<software.amazon.awssdk.services.sqs.model.Message> receiveMessagesFrom(String queueUrl) {
        try {
            return sqsAsyncClient.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .messageAttributeNames("All")
                    .waitTimeSeconds(1)
                    .build()).get().messages();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
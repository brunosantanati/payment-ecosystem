package com.bruno.santana.payout_service.messaging;

import com.bruno.santana.payout_service.domain.PayoutStatus;
import com.bruno.santana.payout_service.exception.NonRetryablePayoutException;
import com.bruno.santana.payout_service.gateway.PayoutGatewayClient;
import com.bruno.santana.payout_service.repository.PayoutRequestRepository;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import io.awspring.cloud.sqs.listener.MessageListenerContainerRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.util.List;
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

    // 1. PostgreSQL 16 real para testar persistência JSONB e PK/Idempotência
    @Container
    static PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("payout_test_db")
            .withUsername("test_user")
            .withPassword("test_pass");

    // 2. ElasticMQ (100% open-source) para emular SQS + DLQ + RedrivePolicy
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

    @Autowired
    private PayoutRequestRepository repository;

    // Mockamos apenas a chamada HTTP externa para o P2A
    @MockitoBean
    private PayoutGatewayClient gatewayClient;

    @DynamicPropertySource
    static void registerDynamicProperties(DynamicPropertyRegistry registry) {
        String sqsEndpoint = "http://" + sqsContainer.getHost() + ":" + sqsContainer.getMappedPort(9324);

        // SQS Properties
        registry.add("spring.cloud.aws.sqs.endpoint", () -> sqsEndpoint);
        registry.add("spring.cloud.aws.region.static", () -> "us-east-1");
        registry.add("spring.cloud.aws.credentials.access-key", () -> "test");
        registry.add("spring.cloud.aws.credentials.secret-key", () -> "test");
        registry.add("app.sqs.payout-commands-queue", () -> MAIN_QUEUE_NAME);
        registry.add("app.sqs.payout-commands-dlq", () -> DLQ_NAME);

        // Sobrescreve o message-visibility de 60s do application.yml para 2s nos testes
        registry.add("spring.cloud.aws.sqs.listener.message-visibility", () -> "2s");

        // PostgreSQL Properties
        registry.add("spring.datasource.url", postgresContainer::getJdbcUrl);
        registry.add("spring.datasource.username", postgresContainer::getUsername);
        registry.add("spring.datasource.password", postgresContainer::getPassword);
    }

    @BeforeAll
    static void createQueuesBeforeSpringContext() throws Exception {
        String endpoint = "http://" + sqsContainer.getHost() + ":" + sqsContainer.getMappedPort(9324);

        try (SqsAsyncClient setupClient = SqsAsyncClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build()) {

            dlqUrl = setupClient.createQueue(CreateQueueRequest.builder()
                    .queueName(DLQ_NAME)
                    .build()).get().queueUrl();

            String dlqArn = setupClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .queueUrl(dlqUrl)
                    .attributeNames(QueueAttributeName.QUEUE_ARN)
                    .build()).get().attributes().get(QueueAttributeName.QUEUE_ARN);

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
    void cleanState() throws Exception {
        repository.deleteAll();
        sqsAsyncClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(mainQueueUrl).build()).get();
        sqsAsyncClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(dlqUrl).build()).get();
    }

    @AfterAll
    static void stopSqsListenersBeforeContainers(@Autowired MessageListenerContainerRegistry registry) {
        registry.stop();
    }

    @Test
    void deveProcessarComandoValidoSalvarNoBancoEDeletarDaFila() throws Exception {
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

        // Verifica se chamou o P2A e persistiu no PostgreSQL com status PAYOUT_SENT_TO_P2A e payload JSONB
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            verify(gatewayClient, times(1)).sendToP2a(command);

            var saved = repository.findById(payoutId);
            assertThat(saved).isPresent();
            assertThat(saved.get().getStatus()).isEqualTo(PayoutStatus.PAYOUT_SENT_TO_P2A);
            assertThat(saved.get().getPayload()).contains(payoutId, correlationId, "WERO", "250.00", DEFAULT_IBAN);
        });

        // Garante que ack.acknowledge() removeu da fila principal e nada caiu na DLQ
        Thread.sleep(2500);
        assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
        assertThat(receiveMessagesFrom(dlqUrl)).isEmpty();
    }

    @Test
    void deveGarantirIdempotenciaQuandoMesmaMensagemForEnviadaDuasVezes() throws Exception {
        String payoutId = UUID.randomUUID().toString();
        PayoutCommandMessage command = new PayoutCommandMessage(
                payoutId,
                UUID.randomUUID().toString(),
                "WERO",
                new BigDecimal("300.00"),
                "EUR",
                DEFAULT_IBAN
        );

        // Envia duas mensagens com o mesmo payoutId (simulando duplicidade do SQS ou Redrive)
        sqsTemplate.send(MAIN_QUEUE_NAME, command);
        sqsTemplate.send(MAIN_QUEUE_NAME, command);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(repository.findById(payoutId)).isPresent();
            assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
        });

        // Garante que o P2A foi chamado apenas 1 vez e existe apenas 1 registro no banco
        verify(gatewayClient, times(1)).sendToP2a(command);
        assertThat(repository.count()).isEqualTo(1);
        assertThat(receiveMessagesFrom(dlqUrl)).isEmpty();
    }

    @Test
    void deveEncaminharPoisonPillDiretoParaDlqSemSalvarNoBanco() {
        PayoutCommandMessage invalidCommand = new PayoutCommandMessage(
                "",
                UUID.randomUUID().toString(),
                "WERO",
                null,
                "EUR",
                DEFAULT_IBAN
        );

        sqsTemplate.send(MAIN_QUEUE_NAME, invalidCommand);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var dlqMessages = receiveMessagesFrom(dlqUrl);
            assertThat(dlqMessages).hasSize(1);
            assertThat(dlqMessages.get(0).messageAttributes().get("dlq-reason").stringValue())
                    .isEqualTo("INVALID_PAYLOAD");
        });

        verify(gatewayClient, never()).sendToP2a(any());
        assertThat(repository.count()).isZero();
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

        doThrow(new NonRetryablePayoutException("ACCOUNT_CLOSED"))
                .when(gatewayClient).sendToP2a(command);

        sqsTemplate.send(MAIN_QUEUE_NAME, command);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var dlqMessages = receiveMessagesFrom(dlqUrl);
            assertThat(dlqMessages).hasSize(1);
            assertThat(dlqMessages.get(0).body()).contains(payoutId);
            assertThat(dlqMessages.get(0).messageAttributes().get("dlq-reason").stringValue())
                    .isEqualTo("ACCOUNT_CLOSED");
        });

        verify(gatewayClient, times(1)).sendToP2a(command);
        assertThat(repository.findById(payoutId)).isEmpty();
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

        doThrow(new RuntimeException("P2A ReadTimeoutException"))
                .when(gatewayClient).sendToP2a(command);

        sqsTemplate.send(MAIN_QUEUE_NAME, command);

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var dlqMessages = receiveMessagesFrom(dlqUrl);
            assertThat(dlqMessages).hasSize(1);
            assertThat(dlqMessages.get(0).body()).contains(payoutId);
        });

        verify(gatewayClient, times(3)).sendToP2a(command);
        assertThat(repository.findById(payoutId)).isEmpty();
        assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
    }

    @Test
    void deveRecuperarComSucessoNaSegundaTentativaSalvarNoBancoENaoSujaDlq() {
        String payoutId = UUID.randomUUID().toString();
        PayoutCommandMessage command = new PayoutCommandMessage(
                payoutId,
                UUID.randomUUID().toString(),
                "WERO",
                new BigDecimal("420.00"),
                "EUR",
                DEFAULT_IBAN
        );

        doThrow(new RuntimeException("HTTP 503 Service Unavailable"))
                .doNothing()
                .when(gatewayClient).sendToP2a(command);

        sqsTemplate.send(MAIN_QUEUE_NAME, command);

        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            verify(gatewayClient, times(2)).sendToP2a(command);
            var saved = repository.findById(payoutId);
            assertThat(saved).isPresent();
            assertThat(saved.get().getStatus()).isEqualTo(PayoutStatus.PAYOUT_SENT_TO_P2A);
        });

        assertThat(receiveMessagesFrom(dlqUrl)).isEmpty();
        assertThat(receiveMessagesFrom(mainQueueUrl)).isEmpty();
    }

    private List<Message> receiveMessagesFrom(String queueUrl) {
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
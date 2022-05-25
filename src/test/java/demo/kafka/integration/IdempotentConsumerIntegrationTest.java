package demo.kafka.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import demo.kafka.event.DemoInboundEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;

import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static demo.kafka.util.TestEventData.buildDemoInboundEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@ExtendWith(LocalDbCreationRule.class)
@EmbeddedKafka(controlledShutdown = true, topics = { "demo-inbound-topic" })
public class IdempotentConsumerIntegrationTest extends IntegrationTestBase {

    @ClassRule
    public static LocalDbCreationRule dynamoDB = new LocalDbCreationRule();

    final static String DEMO_INBOUND_TEST_TOPIC = "demo-inbound-topic";

    @Autowired
    private KafkaTestListener testReceiver;

    @Configuration
    static class TestConfig {

        @Bean
        public KafkaTestListener testReceiver() {
            return new KafkaTestListener();
        }

        @Bean
        public ConsumerFactory<Object, Object> testConsumerFactory(@Value("${kafka.bootstrap-servers}") final String bootstrapServers) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            config.put(ConsumerConfig.GROUP_ID_CONFIG, "IdempotentConsumerIntegrationTest");
            config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            return new DefaultKafkaConsumerFactory<>(config);
        }
    }

    /**
     * Use this receiver to consume messages from the outbound topic.
     */
    public static class KafkaTestListener {
        AtomicInteger counter = new AtomicInteger(0);

        @KafkaListener(groupId = "IdempotentConsumerIntegrationTest", topics = "demo-outbound-topic", autoStartup = "true")
        void receive(@Payload final String payload, @Headers final MessageHeaders headers) {
            log.debug("KafkaTestListener - Received message: " + payload);
            counter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp() {
        super.setUp();
        testReceiver.counter.set(0);
    }

    @Test
    public void testSuccess() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        stubWiremock("/api/kafkawithdynamodbdemo/" + key, 200, "Success");

        sendMessage(DEMO_INBOUND_TEST_TOPIC, eventId, key, buildDemoInboundEvent(key));

        // Check for a message being emitted on demo-outbound-topic
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(1));
        verify(exactly(1), getRequestedFor(urlEqualTo("/api/kafkawithdynamodbdemo/" + key)));
    }

    /**
     * Send in three events and show two are deduplicated by the idempotent consumer, as only one outbound event is emitted.
     */
    @Test
    public void testEventDeduplication() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        stubWiremock("/api/kafkawithdynamodbdemo/" + key, 200, "Success");

        DemoInboundEvent inboundEvent = buildDemoInboundEvent(key);
        sendMessage(DEMO_INBOUND_TEST_TOPIC, eventId, key, inboundEvent);
        sendMessage(DEMO_INBOUND_TEST_TOPIC, eventId, key, inboundEvent);
        sendMessage(DEMO_INBOUND_TEST_TOPIC, eventId, key, inboundEvent);

        // Check for a message being emitted on demo-outbound-topic
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(testReceiver.counter::get, equalTo(1));

        // Now check the duplicate event has been deduplicated.
        TimeUnit.SECONDS.sleep(5);
        assertThat(testReceiver.counter.get(), equalTo(1));
        verify(exactly(1), getRequestedFor(urlEqualTo("/api/kafkawithdynamodbdemo/" + key)));
    }
}

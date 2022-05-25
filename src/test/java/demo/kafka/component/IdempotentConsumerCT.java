package demo.kafka.component;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import dev.lydtech.component.framework.client.kafka.KafkaClient;
import dev.lydtech.component.framework.client.wiremock.WiremockClient;
import dev.lydtech.component.framework.mapper.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static demo.kafka.util.TestEventData.INBOUND_DATA;
import static demo.kafka.util.TestEventData.buildDemoInboundEvent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Prove that the consumer deduplicates the events.
 *
 * The third party service wiremock introduces a short response delay of a few seconds on lookups to ensure events being
 * processed sent in at the same time would be being processed in parallel if not deduplicated.
 */
@Slf4j
public class IdempotentConsumerCT extends ComponentTestBase {

    private static final String TEST_NAME = "IdempotentConsumerComponentTest";

    private Consumer consumer;

    /**
     * Configure the wiremock to return a 503 twice times before success.
     */
    @BeforeEach
    public void setup() {
        consumer = KafkaClient.getInstance().createConsumer(TEST_NAME, DEMO_OUTBOUND_TOPIC);

        WiremockClient.getInstance().resetMappings();
        WiremockClient.getInstance().postMappingFile("thirdParty/success_short_delay.json");

        // Clear the topic.
        consumer.poll(Duration.ofSeconds(1));
    }

    @AfterEach
    public void tearDown() {
        consumer.close();
    }

    /**
     * Send multiple events in parallel.  As they share the same key they will be consumed by the same consumer and so
     * processed serially.  They should therefore be successfully deduplicated by the idempotent consumer, using
     * DynamoDB as the backing store.
     */
    @Test
    public void testIdempotentConsumer() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        String payload  = JsonMapper.writeToJson(buildDemoInboundEvent(key));

        KafkaClient.getInstance().sendMessage(DEMO_INBOUND_TOPIC, key, payload, Collections.singletonMap("demo_eventIdHeader", eventId));
        KafkaClient.getInstance().sendMessage(DEMO_INBOUND_TOPIC, key, payload, Collections.singletonMap("demo_eventIdHeader", eventId));
        KafkaClient.getInstance().sendMessage(DEMO_INBOUND_TOPIC, key, payload, Collections.singletonMap("demo_eventIdHeader", eventId));

        List<ConsumerRecord<String, String>> outboundEvents = KafkaClient.getInstance().consumeAndAssert(TEST_NAME, consumer, 1, 5);
        assertThat(outboundEvents.get(0).key(), equalTo(key));
        assertThat(outboundEvents.get(0).value(), containsString(INBOUND_DATA));
    }
}

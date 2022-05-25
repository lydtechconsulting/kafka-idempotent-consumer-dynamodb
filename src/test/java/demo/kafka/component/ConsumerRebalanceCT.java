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

@Slf4j
public class ConsumerRebalanceCT extends ComponentTestBase {

    private static final String TEST_NAME = "ConsumerRebalanceComponentTest";

    private Consumer consumer;

    /**
     * Configure the third party service wiremock to delay on the first attempt causing the poll timeout to be exceeded.
     *
     * This means the second consumer instance will poll the same message, a duplicate.
     */
    @BeforeEach
    public void setUp() {
        consumer = KafkaClient.getInstance().createConsumer(TEST_NAME, DEMO_OUTBOUND_TOPIC);

        WiremockClient.getInstance().resetMappings();
        WiremockClient.getInstance().postMappingFile("thirdParty/delay_behaviour_01_start-to-success.json");
        WiremockClient.getInstance().postMappingFile("thirdParty/delay_behaviour_02_success.json");

        // Clear the topic.
        consumer.poll(Duration.ofSeconds(1));
    }

    @AfterEach
    public void tearDown() {
        consumer.close();
    }

    /**
     * Demonstrate that when a consumer poll times out whilst the message is being processed, when the same message is
     * consumed by a second consumer instance, that the deduplication check does not work.
     *
     * Two or more instances of the service must be running (configure in TestContainersExecutionListener).
     *
     * The third-party-service wiremock is configured to delay before a successful response.
     *
     * Consumer poll timeout is 10 seconds, with 3PP delay of 15 seconds.
     *
     * The delay exceeds the poll timeout, so the consumer group is rebalanced and the message is redelivered to the
     * second service instance.
     *
     * The second call to the 3PP wiremock delays for 3 seconds, causing both messages to be processed in parallel.
     *
     * Note that the extra seconds for the delay for the consumer group rebalance account for some of the time.
     *
     * The upshot is that two outbound events are emitted for the single inbound event.
     */
    @Test
    public void testConsumerRebalanceDeduplication() throws Exception {
        String key = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        String payload  = JsonMapper.writeToJson(buildDemoInboundEvent(key));

        KafkaClient.getInstance().sendMessage(DEMO_INBOUND_TOPIC, key, payload, Collections.singletonMap("demo_eventIdHeader", eventId));

        List<ConsumerRecord<String, String>> outboundEvents = KafkaClient.getInstance().consumeAndAssert(TEST_NAME, consumer, 2, 5);
        assertThat(outboundEvents.get(0).key(), equalTo(key));
        assertThat(outboundEvents.get(0).value(), containsString(INBOUND_DATA));
        assertThat(outboundEvents.get(1).key(), equalTo(key));
        assertThat(outboundEvents.get(1).value(), containsString(INBOUND_DATA));
    }
}

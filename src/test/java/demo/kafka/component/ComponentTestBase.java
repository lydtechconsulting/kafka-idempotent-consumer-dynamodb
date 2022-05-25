package demo.kafka.component;

import demo.kafka.domain.ProcessedEvent;
import dev.lydtech.component.framework.client.localstack.DynamoDbClient;
import dev.lydtech.component.framework.extension.TestContainersSetupExtension;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@Slf4j
@ExtendWith(TestContainersSetupExtension.class)
public abstract class ComponentTestBase {

    private static final String REGION = "eu-west-2";

    protected static final String DEMO_INBOUND_TOPIC = "demo-inbound-topic";
    protected static final String DEMO_OUTBOUND_TOPIC = "demo-outbound-topic";


    /**
     * Create the DynamoDb tables for the test.
     */
    @BeforeAll
    public static void setUpOnce() {
        DynamoDbClient.getInstance().createTable(ProcessedEvent.class, REGION);
    }
}

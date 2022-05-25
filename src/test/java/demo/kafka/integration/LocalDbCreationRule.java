package demo.kafka.integration;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

@Slf4j
public class LocalDbCreationRule implements BeforeAllCallback, AfterAllCallback {
    private DynamoDBProxyServer server;

    public LocalDbCreationRule() {
        System.setProperty("sqlite4java.library.path", "native-libs");
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        try {
            String port = getAvailablePort();
            server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", port});
            server.start();
        } catch(Exception e) {
            log.error("LocalDbCreationRule start failed", e);
            throw e;
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        this.stopUnchecked(server);
    }

    public String getAvailablePort() throws Exception {
        // ServerSocket serverSocket = new ServerSocket(0);
        // String port = String.valueOf(serverSocket.getLocalPort());
        // return port;
        return "8000";
    }

    protected void stopUnchecked(DynamoDBProxyServer dynamoDbServer) {
        try {
            dynamoDbServer.stop();
        } catch (Exception e) {
            log.error("LocalDbCreationRule stop failed", e);
            throw new RuntimeException(e);
        }
    }
}

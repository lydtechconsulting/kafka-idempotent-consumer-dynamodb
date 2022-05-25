package demo.kafka.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import demo.kafka.domain.ProcessedEvent;
import demo.kafka.event.DemoInboundEvent;
import demo.kafka.exception.DuplicateEventException;
import demo.kafka.exception.KafkaDemoException;
import demo.kafka.exception.KafkaDemoRetryableException;
import demo.kafka.lib.KafkaClient;
import demo.kafka.properties.KafkaDemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Service
@Slf4j
@RequiredArgsConstructor
public class DemoService {;

    @Autowired
    private KafkaDemoProperties properties;

    @Autowired
    private KafkaClient kafkaClient;

    @Autowired
    private DynamoDBMapper dynamoDBMapper;

    public void process(String eventId, String key, DemoInboundEvent event) {
        try {
            // 1. Check if the event is a duplicate.
            Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
            eav.put(":v1", new AttributeValue().withS(eventId));
            DynamoDBQueryExpression<ProcessedEvent> queryExpression = new DynamoDBQueryExpression<ProcessedEvent>()
                    .withKeyConditionExpression("Id = :v1")
                    .withExpressionAttributeValues(eav);
            List<ProcessedEvent> duplicateEventIds = dynamoDBMapper.query(ProcessedEvent.class, queryExpression);
            if(duplicateEventIds.size()>0) {
                log.info("Duplicate event received: " + eventId);
                throw new DuplicateEventException(eventId);
            }

            // 2. Perform the event processing.
            callThirdparty(key);
            kafkaClient.sendMessage(key, event.getData());

            // 3. Record the processed event Id to allow duplicates to be detected.
            ProcessedEvent processedEvent = new ProcessedEvent(eventId);
            DynamoDBSaveExpression saveExpression = new DynamoDBSaveExpression()
                    .withExpectedEntry("Id", new ExpectedAttributeValue().withExists(false));
            dynamoDBMapper.save(processedEvent, saveExpression);
            log.debug("Event persisted with Id: {}", eventId);
        } catch (ConditionalCheckFailedException e) {
            log.info("ConditionalCheckFailedException Error: " + e.getMessage());
            throw new DuplicateEventException(eventId);
        } catch (DuplicateEventException e) {
            throw e;
        } catch (Exception e) {
            log.error("Exception thrown: " + e.getMessage(), e);
            throw e;
        }
    }

    private void callThirdparty(String key) {
        RestTemplate restTemplate = new RestTemplate();
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(properties.getThirdpartyEndpoint() + "/" + key, String.class);
            if (response.getStatusCodeValue() != 200) {
                throw new RuntimeException("error " + response.getStatusCodeValue());
            }
            return;
        } catch (HttpServerErrorException e) {
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ") with an error code of " + e.getRawStatusCode(), e);   // e.getRawStatusCode()
            throw new KafkaDemoRetryableException(e);
        } catch (ResourceAccessException e) {
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ")", e);
            throw new KafkaDemoRetryableException(e);
        } catch (Exception e) {
            log.error("Error calling thirdparty api, returned an (" + e.getClass().getName() + ")", e);
            throw new KafkaDemoException(e);
        }
    }
}

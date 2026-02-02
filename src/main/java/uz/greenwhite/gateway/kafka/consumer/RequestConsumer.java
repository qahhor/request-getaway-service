package uz.greenwhite.gateway.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import uz.greenwhite.gateway.http.HttpRequestService;
import uz.greenwhite.gateway.kafka.producer.RequestProducer;
import uz.greenwhite.gateway.model.enums.ErrorSource;
import uz.greenwhite.gateway.model.enums.RequestStatus;
import uz.greenwhite.gateway.model.kafka.RequestMessage;
import uz.greenwhite.gateway.model.kafka.ResponseMessage;
import uz.greenwhite.gateway.state.RequestStateService;

@Slf4j
@Service
@RequiredArgsConstructor
public class RequestConsumer {

    private final HttpRequestService httpRequestService;
    private final RequestStateService requestStateService;
    private final RequestProducer requestProducer;

    private static final int MAX_RETRY_ATTEMPTS = 3;

    /**
     * Consume new requests from Kafka and send HTTP requests
     */
    @KafkaListener(
            topics = "${gateway.kafka.topics.request-new}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRequest(ConsumerRecord<String, RequestMessage> record, Acknowledgment ack) {
        String key = record.key();
        RequestMessage message = record.value();

        log.info("Received request: {} [partition={}, offset={}]",
                key, record.partition(), record.offset());

        try {
            // 1. Check if already completed (idempotency)
            if (requestStateService.isCompleted(key)) {
                log.warn("Request already completed, skipping: {}", key);
                ack.acknowledge();
                return;
            }

            // 2. Try to acquire lock
            if (!requestStateService.tryLock(key)) {
                log.warn("Request is being processed by another instance: {}", key);
                ack.acknowledge();
                return;
            }

            try {
                // 3. Create or get state
                requestStateService.createInitialState(key);

                // 4. Update status to SENT
                requestStateService.updateStatus(key, RequestStatus.SENT);

                // 5. Send HTTP request
                ResponseMessage response = httpRequestService.sendRequest(message).block();

                // 6. Check response
                if (response != null && response.isSuccess()) {
                    // Success - send to response topic
                    requestProducer.sendResponse(response);
                    requestStateService.updateStatus(key, RequestStatus.COMPLETED);
                    log.info("Request processed successfully: {}", key);
                } else {
                    // Error - check if retryable
                    handleFailedResponse(key, message, response);
                }

                // 7. Acknowledge
                ack.acknowledge();

            } finally {
                // Release lock
                requestStateService.releaseLock(key);
            }

        } catch (Exception e) {
            log.error("Error processing request {}: {}", key, e.getMessage(), e);
            handleException(key, message, e, ack);
        }
    }

    /**
     * Handle failed HTTP response
     */
    private void handleFailedResponse(String key, RequestMessage message, ResponseMessage response) {
        int attemptCount = requestStateService.incrementAttempt(key);

        if (response != null && response.isRetryable() && attemptCount < MAX_RETRY_ATTEMPTS) {
            log.warn("Retryable error for {}, attempt {}/{}", key, attemptCount, MAX_RETRY_ATTEMPTS);
            // Re-send to retry topic or just let Kafka redeliver
        } else {
            // Max retries exceeded or non-retryable error
            String error = response != null ? response.getErrorMessage() : "Unknown error";
            requestStateService.markFailed(key, error, ErrorSource.HTTP);
            requestProducer.sendToDlq(key, message, error);
            log.error("Request failed permanently: {} - {}", key, error);
        }
    }

    /**
     * Handle exception during processing
     */
    private void handleException(String key, RequestMessage message, Exception e, Acknowledgment ack) {
        int attemptCount = requestStateService.incrementAttempt(key);

        if (attemptCount < MAX_RETRY_ATTEMPTS) {
            log.warn("Exception for {}, attempt {}/{}, will retry", key, attemptCount, MAX_RETRY_ATTEMPTS);
            // Don't acknowledge - message will be redelivered
        } else {
            requestStateService.markFailed(key, e.getMessage(), ErrorSource.SYSTEM);
            requestProducer.sendToDlq(key, message, e.getMessage());
            ack.acknowledge(); // Acknowledge to prevent infinite loop
        }
    }
}
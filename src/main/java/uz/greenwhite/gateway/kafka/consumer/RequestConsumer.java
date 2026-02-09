package uz.greenwhite.gateway.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import uz.greenwhite.gateway.config.RetryProperties;
import uz.greenwhite.gateway.http.HttpRequestService;
import uz.greenwhite.gateway.kafka.producer.RequestProducer;
import uz.greenwhite.gateway.model.enums.ErrorSource;
import uz.greenwhite.gateway.model.enums.RequestStatus;
import uz.greenwhite.gateway.model.kafka.RequestMessage;
import uz.greenwhite.gateway.model.kafka.ResponseMessage;
import uz.greenwhite.gateway.state.RequestStateService;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class RequestConsumer {

    private final HttpRequestService httpRequestService;
    private final RequestStateService requestStateService;
    private final RequestProducer requestProducer;
    private final RetryProperties retryProperties;
    private final ThreadPoolTaskExecutor httpExecutor;

    public RequestConsumer(
            HttpRequestService httpRequestService,
            RequestStateService requestStateService,
            RequestProducer requestProducer,
            RetryProperties retryProperties,
            @Qualifier("httpRequestExecutor") ThreadPoolTaskExecutor httpExecutor) {
        this.httpRequestService = httpRequestService;
        this.requestStateService = requestStateService;
        this.requestProducer = requestProducer;
        this.retryProperties = retryProperties;
        this.httpExecutor = httpExecutor;
    }

    /**
     * Consume new requests from Kafka and delegate HTTP work to thread pool
     */
    @KafkaListener(
            id = "requestConsumer",
            topics = "${gateway.kafka.topics.request-new}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "requestConsumerFactory"
    )
    public void consumeRequest(ConsumerRecord<String, RequestMessage> record, Acknowledgment ack) {
        String key = record.key();
        RequestMessage message = record.value();

        log.info("Received request: {} [partition={}, offset={}]",
                key, record.partition(), record.offset());

        try {
            // 1. Idempotency check
            if (requestStateService.isCompleted(key)) {
                log.warn("Request already completed, skipping: {}", key);
                ack.acknowledge();
                return;
            }

            // 2. Concurrency lock
            if (!requestStateService.tryLock(key)) {
                log.warn("Request is being processed by another instance: {}", key);
                ack.acknowledge();
                return;
            }

            // 3. HTTP ishni alohida thread pool ga topshirish
            CompletableFuture.runAsync(() -> {
                try {
                    processRequest(key, message);
                } catch (Exception e) {
                    log.error("Async processing failed for {}: {}", key, e.getMessage(), e);
                    handleFailedProcessing(key, message, e);
                } finally {
                    requestStateService.releaseLock(key);
                }
            }, httpExecutor).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("Unexpected error in async processing for {}: {}",
                            key, throwable.getMessage(), throwable);
                }
                // Acknowledge after processing (success or permanent failure)
                ack.acknowledge();
            });

        } catch (Exception e) {
            log.error("Error submitting request {}: {}", key, e.getMessage(), e);
            requestStateService.releaseLock(key);
            ack.acknowledge();
        }
    }

    /**
     * Process single request (runs on httpExecutor thread)
     */
    private void processRequest(String key, RequestMessage message) {
        // 1. Create initial state
        requestStateService.createInitialState(key);

        // 2. Update status to SENT
        requestStateService.updateStatus(key, RequestStatus.SENT);

        // 3. Send HTTP request
        ResponseMessage response = httpRequestService.sendRequest(message).block();

        // 4. Handle response
        if (response != null && response.isSuccess()) {
            handleSuccess(key, response);
        } else {
            handleFailedResponse(key, message, response);
        }
    }

    private void handleSuccess(String key, ResponseMessage response) {
        requestProducer.sendResponse(response);
        requestStateService.updateStatus(key, RequestStatus.COMPLETED);
        log.info("Request processed successfully: {}", key);
    }

    private void handleFailedResponse(String key, RequestMessage message, ResponseMessage response) {
        int attemptCount = requestStateService.incrementAttempt(key);
        int httpStatus = response != null ? response.getHttpStatus() : 0;
        String errorMessage = response != null ? response.getErrorMessage() : "Unknown error";

        boolean isRetryable = retryProperties.isRetryable(httpStatus);
        boolean hasAttemptsLeft = attemptCount < retryProperties.getMaxAttempts();

        if (isRetryable && hasAttemptsLeft) {
            log.warn("Retryable error for {}: status={}, attempt {}/{}",
                    key, httpStatus, attemptCount, retryProperties.getMaxAttempts());
            // Re-send to Kafka for retry (instead of blocking consumer thread)
            requestProducer.sendRequest(message);
        } else {
            handlePermanentFailure(key, message, httpStatus, errorMessage, ErrorSource.HTTP);
        }
    }

    private void handleFailedProcessing(String key, RequestMessage message, Exception e) {
        int attemptCount = requestStateService.incrementAttempt(key);

        if (attemptCount < retryProperties.getMaxAttempts()) {
            log.warn("Processing error for {}, attempt {}/{}, re-sending",
                    key, attemptCount, retryProperties.getMaxAttempts());
            requestProducer.sendRequest(message);
        } else {
            handlePermanentFailure(key, message, 0, e.getMessage(), ErrorSource.SYSTEM);
        }
    }

    private void handlePermanentFailure(String key, RequestMessage message,
                                        int httpStatus, String errorMessage, ErrorSource source) {
        log.error("Request failed permanently: {} - status={}, error={}, source={}",
                key, httpStatus, errorMessage, source);
        requestProducer.sendToDlq(key, message, "[" + source + "] " + errorMessage);
        requestStateService.updateStatus(key, RequestStatus.FAILED);
    }
}
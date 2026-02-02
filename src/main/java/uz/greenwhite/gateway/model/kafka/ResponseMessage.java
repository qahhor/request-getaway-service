package uz.greenwhite.gateway.model.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Kafka message for responses (bmb.request.response topic)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResponseMessage {

    private Long companyId;
    private Long requestId;

    // HTTP Response
    private int httpStatus;
    private String contentType;
    private String body;

    // Error (if failed)
    private String errorMessage;
    private String errorSource;  // HTTP, CALLBACK, SYSTEM
    private String errorCode;

    // Timestamps
    private LocalDateTime processedAt;

    /**
     * Composite ID for Kafka key
     */
    public String getCompositeId() {
        return companyId + ":" + requestId;
    }

    /**
     * Check if success
     */
    public boolean isSuccess() {
        return httpStatus >= 200 && httpStatus < 300 && errorMessage == null;
    }

    /**
     * Check if error is retryable (5xx or 429)
     */
    public boolean isRetryable() {
        return httpStatus == 429 || httpStatus >= 500;
    }
}
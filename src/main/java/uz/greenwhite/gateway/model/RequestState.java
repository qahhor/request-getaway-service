package uz.greenwhite.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import uz.greenwhite.gateway.model.enums.ErrorSource;
import uz.greenwhite.gateway.model.enums.RequestStatus;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestState implements Serializable {

    private String compositeId;  // companyId:requestId
    private RequestStatus status;

    // Retry tracking
    private int attemptCount;
    private String lastError;
    private ErrorSource errorSource;

    // Kafka metadata
    private Long kafkaOffset;
    private Integer kafkaPartition;

    // Timestamps
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    /**
     * Increment attempt count
     */
    public void incrementAttempt() {
        this.attemptCount++;
        this.updatedAt = LocalDateTime.now();
    }

    /**
     * Mark as failed
     */
    public void markFailed(String error, ErrorSource source) {
        this.status = RequestStatus.FAILED;
        this.lastError = error;
        this.errorSource = source;
        this.updatedAt = LocalDateTime.now();
    }

    /**
     * Update status
     */
    public void updateStatus(RequestStatus newStatus) {
        this.status = newStatus;
        this.updatedAt = LocalDateTime.now();
    }
}
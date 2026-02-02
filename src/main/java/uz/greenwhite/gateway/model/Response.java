package uz.greenwhite.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import uz.greenwhite.gateway.model.enums.ErrorSource;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Response {

    private Long companyId;
    private Long requestId;

    // HTTP Response
    private int httpStatus;
    private String contentType;
    private String body;

    // Error details (if failed)
    private String errorMessage;
    private ErrorSource errorSource;
    private String errorCode;

    // Timestamps
    private LocalDateTime processedAt;

    /**
     * Composite ID: companyId:requestId
     */
    public String getCompositeId() {
        return companyId + ":" + requestId;
    }

    /**
     * Check if response is successful
     */
    public boolean isSuccess() {
        return httpStatus >= 200 && httpStatus < 300;
    }

    /**
     * Check if response is retryable
     */
    public boolean isRetryable() {
        return httpStatus == 429 || httpStatus == 503 || httpStatus >= 500;
    }
}
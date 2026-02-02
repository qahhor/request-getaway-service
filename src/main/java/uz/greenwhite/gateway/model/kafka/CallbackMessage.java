package uz.greenwhite.gateway.model.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Kafka message for callback execution (bmb.request.callback topic)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CallbackMessage {

    private Long companyId;
    private Long requestId;

    // Callback details
    private String callbackProcedure;
    private String response;
    private String errorMessage;

    // Timestamps
    private LocalDateTime savedAt;

    /**
     * Composite ID for Kafka key
     */
    public String getCompositeId() {
        return companyId + ":" + requestId;
    }
}
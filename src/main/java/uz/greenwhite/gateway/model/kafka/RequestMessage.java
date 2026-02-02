package uz.greenwhite.gateway.model.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Kafka message for new requests (bmb.request.new topic)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RequestMessage {

    private Long companyId;
    private Long requestId;
    private Long filialId;
    private Long endpointId;

    // HTTP details
    private String baseUrl;
    private String uri;
    private String params;
    private String method;
    private Map<String, String> headers;
    private String body;

    // OAuth2
    private String oauth2Provider;

    // Callback
    private String callbackProcedure;

    // Metadata
    private String projectCode;
    private String sourceTable;
    private Long sourceId;

    // Timestamps
    private LocalDateTime createdAt;

    /**
     * Composite ID for Kafka key
     */
    public String getCompositeId() {
        return companyId + ":" + requestId;
    }
}
package uz.greenwhite.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Request {

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
    private LocalDateTime createdOn;

    /**
     * Composite ID: companyId:requestId
     */
    public String getCompositeId() {
        return companyId + ":" + requestId;
    }

    /**
     * Full URL: baseUrl + uri + params
     */
    public String getFullUrl() {
        StringBuilder url = new StringBuilder(baseUrl);
        if (uri != null) {
            url.append(uri);
        }
        if (params != null) {
            url.append(params);
        }
        return url.toString();
    }
}
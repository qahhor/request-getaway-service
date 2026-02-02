package uz.greenwhite.gateway.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "gateway.biruni")
public class GatewayProperties {

    /**
     * Biruni application base URL
     * Example: http://localhost:8081/b6/b
     */
    private String baseUrl;

    /**
     * Username for Basic Auth
     */
    private String username;

    /**
     * Password for Basic Auth
     */
    private String password;

    /**
     * URI for pulling requests
     * Example: /biruni/bmb/requests$pull
     */
    private String requestPullUri = "/biruni/bmb/requests$pull";

    /**
     * URI for saving response
     * Example: /biruni/bmb/requests$save
     */
    private String responseSaveUri = "/biruni/bmb/requests$save";

    /**
     * Connection timeout in seconds
     */
    private int connectionTimeout = 60;
}
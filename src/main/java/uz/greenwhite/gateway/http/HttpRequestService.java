package uz.greenwhite.gateway.http;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import uz.greenwhite.gateway.model.kafka.RequestMessage;
import uz.greenwhite.gateway.model.kafka.ResponseMessage;

import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class HttpRequestService {

    private final WebClient webClient;

    /**
     * Send HTTP request to external API
     */
    public Mono<ResponseMessage> sendRequest(RequestMessage request) {
        String compositeId = request.getCompositeId();
        String fullUrl = buildFullUrl(request);
        HttpMethod method = HttpMethod.valueOf(request.getMethod().toUpperCase());

        log.info("Sending HTTP request: {} {} -> {}", method, fullUrl, compositeId);

        return webClient
                .method(method)
                .uri(fullUrl)
                .headers(headers -> addHeaders(headers, request.getHeaders()))
                .bodyValue(request.getBody() != null ? request.getBody() : "")
                .exchangeToMono(response -> {
                    int statusCode = response.statusCode().value();
                    String contentType = response.headers().contentType()
                            .map(MediaType::toString)
                            .orElse("application/json");

                    return response.bodyToMono(String.class)
                            .defaultIfEmpty("")
                            .map(body -> buildSuccessResponse(request, statusCode, contentType, body));
                })
                .doOnSuccess(resp -> log.info("HTTP response received: {} -> status={}",
                        compositeId, resp.getHttpStatus()))
                .doOnError(ex -> log.error("HTTP request failed: {} -> {}",
                        compositeId, ex.getMessage()))
                .onErrorResume(ex -> Mono.just(buildErrorResponse(request, ex)));
    }

    /**
     * Build full URL from request
     */
    private String buildFullUrl(RequestMessage request) {
        StringBuilder url = new StringBuilder(request.getBaseUrl());

        if (request.getUri() != null && !request.getUri().isEmpty()) {
            url.append(request.getUri());
        }

        if (request.getParams() != null && !request.getParams().isEmpty()) {
            url.append(request.getParams());
        }

        return url.toString();
    }

    /**
     * Add headers to request
     */
    private void addHeaders(HttpHeaders httpHeaders, Map<String, String> customHeaders) {
        // Default headers
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);

        // Custom headers from request
        if (customHeaders != null) {
            customHeaders.forEach(httpHeaders::add);
        }
    }

    /**
     * Build success response
     */
    private ResponseMessage buildSuccessResponse(RequestMessage request, int status,
                                                 String contentType, String body) {
        return ResponseMessage.builder()
                .companyId(request.getCompanyId())
                .requestId(request.getRequestId())
                .httpStatus(status)
                .contentType(contentType)
                .body(body)
                .processedAt(LocalDateTime.now())
                .build();
    }

    /**
     * Build error response
     */
    private ResponseMessage buildErrorResponse(RequestMessage request, Throwable ex) {
        int httpStatus = 500;
        String errorMessage = ex.getMessage();

        if (ex instanceof WebClientResponseException wcEx) {
            httpStatus = wcEx.getStatusCode().value();
            errorMessage = wcEx.getResponseBodyAsString();
        }

        return ResponseMessage.builder()
                .companyId(request.getCompanyId())
                .requestId(request.getRequestId())
                .httpStatus(httpStatus)
                .errorMessage(errorMessage)
                .errorSource("HTTP")
                .processedAt(LocalDateTime.now())
                .build();
    }
}
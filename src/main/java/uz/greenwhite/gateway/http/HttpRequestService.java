package uz.greenwhite.gateway.http;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
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
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    private CircuitBreaker circuitBreaker;

    @PostConstruct
    public void init() {
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("externalApi");

        // State o'zgarishlarini log qilish
        circuitBreaker.getEventPublisher()
                .onStateTransition(event ->
                        log.warn("⚡ Circuit Breaker state change: {}", event.getStateTransition()))
                .onFailureRateExceeded(event ->
                        log.warn("⚠ Circuit Breaker failure rate exceeded: {}%", event.getFailureRate()))
                .onSlowCallRateExceeded(event ->
                        log.warn("⚠ Circuit Breaker slow call rate exceeded: {}%", event.getSlowCallRate()));
    }

    /**
     * Send HTTP request with Circuit Breaker protection
     */
    public Mono<ResponseMessage> sendRequest(RequestMessage request) {
        String compositeId = request.getCompositeId();

        // 1. Circuit Breaker OPEN bo'lsa — darhol reject
        try {
            circuitBreaker.acquirePermission();
        } catch (CallNotPermittedException ex) {
            log.warn("Circuit breaker OPEN — request blocked: {}", compositeId);
            return Mono.just(buildCircuitBreakerResponse(request));
        }

        long startTime = System.nanoTime();
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
                .doOnSuccess(resp -> {
                    long duration = System.nanoTime() - startTime;
                    int status = resp.getHttpStatus();

                    if (status >= 200 && status < 500) {
                        // 2xx, 3xx, 4xx — API ishlayapti, CB success
                        circuitBreaker.onSuccess(duration, java.util.concurrent.TimeUnit.NANOSECONDS);
                        if (status >= 400) {
                            log.warn("HTTP client error (not CB failure): {} -> status={}", compositeId, status);
                        } else {
                            log.info("HTTP success: {} -> status={}", compositeId, status);
                        }
                    } else {
                        // 5xx — server error, CB failure
                        circuitBreaker.onError(duration, java.util.concurrent.TimeUnit.NANOSECONDS,
                                new RuntimeException("HTTP " + status));
                        log.warn("HTTP server error (CB failure): {} -> status={}", compositeId, status);
                    }
                })
                .onErrorResume(ex -> {
                    long duration = System.nanoTime() - startTime;
                    circuitBreaker.onError(duration, java.util.concurrent.TimeUnit.NANOSECONDS, ex);
                    log.error("HTTP request failed: {} -> {}", compositeId, ex.getMessage());
                    return Mono.just(buildErrorResponse(request, ex));
                });
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
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        if (customHeaders != null) {
            customHeaders.forEach(httpHeaders::add);
        }
    }

    /**
     * Circuit Breaker OPEN response
     */
    private ResponseMessage buildCircuitBreakerResponse(RequestMessage request) {
        return ResponseMessage.builder()
                .companyId(request.getCompanyId())
                .requestId(request.getRequestId())
                .httpStatus(503)
                .errorMessage("Circuit breaker is OPEN: external API unavailable")
                .errorSource("CIRCUIT_BREAKER")
                .processedAt(LocalDateTime.now())
                .build();
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
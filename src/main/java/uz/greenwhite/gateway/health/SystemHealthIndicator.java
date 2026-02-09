package uz.greenwhite.gateway.health;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import uz.greenwhite.gateway.concurrency.ConcurrencyMonitorService;
import uz.greenwhite.gateway.concurrency.DynamicConcurrencyManager;

import static uz.greenwhite.gateway.concurrency.ConcurrencyMonitorService.REQUEST_LISTENER_ID;
import static uz.greenwhite.gateway.concurrency.ConcurrencyMonitorService.RESPONSE_LISTENER_ID;

@Slf4j
@Component("gateway")
public class SystemHealthIndicator implements HealthIndicator {

    private final ConcurrencyMonitorService monitorService;
    private final DynamicConcurrencyManager concurrencyManager;
    private final ThreadPoolTaskExecutor httpExecutor;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    public SystemHealthIndicator(
            ConcurrencyMonitorService monitorService,
            DynamicConcurrencyManager concurrencyManager,
            @Qualifier("httpRequestExecutor") ThreadPoolTaskExecutor httpExecutor,
            CircuitBreakerRegistry circuitBreakerRegistry) {
        this.monitorService = monitorService;
        this.concurrencyManager = concurrencyManager;
        this.httpExecutor = httpExecutor;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    @Override
    public Health health() {
        CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker("externalApi");
        CircuitBreaker.Metrics cbMetrics = cb.getMetrics();

        boolean circuitBreakerOpen = cb.getState() == CircuitBreaker.State.OPEN;

        Health.Builder builder = circuitBreakerOpen ? Health.down() : Health.up();

        return builder
                // Kafka Consumer
                .withDetail("kafka.requestConsumer.concurrency",
                        concurrencyManager.getCurrentConcurrency(REQUEST_LISTENER_ID))
                .withDetail("kafka.requestConsumer.lag",
                        monitorService.getLastKnownLag("bmb.request.new"))
                .withDetail("kafka.responseConsumer.concurrency",
                        concurrencyManager.getCurrentConcurrency(RESPONSE_LISTENER_ID))
                .withDetail("kafka.responseConsumer.lag",
                        monitorService.getLastKnownLag("bmb.request.response"))
                // HTTP Thread Pool
                .withDetail("http.pool.active", httpExecutor.getActiveCount())
                .withDetail("http.pool.size", httpExecutor.getThreadPoolExecutor().getPoolSize())
                .withDetail("http.pool.queue", httpExecutor.getThreadPoolExecutor().getQueue().size())
                // Circuit Breaker
                .withDetail("circuitBreaker.state", cb.getState().name())
                .withDetail("circuitBreaker.failureRate", cbMetrics.getFailureRate() + "%")
                .withDetail("circuitBreaker.slowCallRate", cbMetrics.getSlowCallRate() + "%")
                .withDetail("circuitBreaker.bufferedCalls", cbMetrics.getNumberOfBufferedCalls())
                .withDetail("circuitBreaker.failedCalls", cbMetrics.getNumberOfFailedCalls())
                .build();
    }
}
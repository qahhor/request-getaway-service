package uz.greenwhite.gateway.concurrency;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import static uz.greenwhite.gateway.concurrency.ConcurrencyMonitorService.REQUEST_LISTENER_ID;
import static uz.greenwhite.gateway.concurrency.ConcurrencyMonitorService.RESPONSE_LISTENER_ID;

@Slf4j
@Component
public class ConcurrencyMetrics {

    private final MeterRegistry meterRegistry;
    private final DynamicConcurrencyManager concurrencyManager;
    private final ConcurrencyMonitorService monitorService;
    private final ThreadPoolTaskExecutor httpExecutor;

    @Value("${gateway.kafka.topics.request-new}")
    private String requestNewTopic;

    @Value("${gateway.kafka.topics.request-response}")
    private String requestResponseTopic;

    public ConcurrencyMetrics(
            MeterRegistry meterRegistry,
            DynamicConcurrencyManager concurrencyManager,
            ConcurrencyMonitorService monitorService,
            @Qualifier("httpRequestExecutor") ThreadPoolTaskExecutor httpExecutor) {
        this.meterRegistry = meterRegistry;
        this.concurrencyManager = concurrencyManager;
        this.monitorService = monitorService;
        this.httpExecutor = httpExecutor;
    }

    @PostConstruct
    public void registerMetrics() {

        // ==================== REQUEST CONSUMER METRICS ====================

        Gauge.builder("gateway.kafka.consumer.concurrency",
                        () -> concurrencyManager.getCurrentConcurrency(REQUEST_LISTENER_ID))
                .description("Current Kafka consumer concurrency level")
                .tag("listener", REQUEST_LISTENER_ID)
                .register(meterRegistry);

        Gauge.builder("gateway.kafka.consumer.lag",
                        () -> monitorService.getLastKnownLag(requestNewTopic))
                .description("Current Kafka consumer lag")
                .tag("topic", "request-new")
                .tag("listener", REQUEST_LISTENER_ID)
                .register(meterRegistry);

        // ==================== RESPONSE CONSUMER METRICS ====================

        Gauge.builder("gateway.kafka.consumer.concurrency",
                        () -> concurrencyManager.getCurrentConcurrency(RESPONSE_LISTENER_ID))
                .description("Current Kafka consumer concurrency level")
                .tag("listener", RESPONSE_LISTENER_ID)
                .register(meterRegistry);

        Gauge.builder("gateway.kafka.consumer.lag",
                        () -> monitorService.getLastKnownLag(requestResponseTopic))
                .description("Current Kafka consumer lag")
                .tag("topic", "request-response")
                .tag("listener", RESPONSE_LISTENER_ID)
                .register(meterRegistry);

        // ==================== HTTP THREAD POOL METRICS ====================

        Gauge.builder("gateway.http.pool.active", httpExecutor::getActiveCount)
                .description("Currently active HTTP request threads")
                .register(meterRegistry);

        Gauge.builder("gateway.http.pool.size", httpExecutor.getThreadPoolExecutor()::getPoolSize)
                .description("Current HTTP thread pool size")
                .register(meterRegistry);

        Gauge.builder("gateway.http.pool.queue", () -> httpExecutor.getThreadPoolExecutor().getQueue().size())
                .description("Tasks waiting in HTTP thread pool queue")
                .register(meterRegistry);

        Gauge.builder("gateway.http.pool.completed",
                        httpExecutor.getThreadPoolExecutor()::getCompletedTaskCount)
                .description("Total completed HTTP tasks")
                .register(meterRegistry);

        log.info("Concurrency metrics registered for [{}] and [{}]",
                REQUEST_LISTENER_ID, RESPONSE_LISTENER_ID);
    }
}
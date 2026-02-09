package uz.greenwhite.gateway.config;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class GracefulShutdownConfig {

    private final KafkaListenerEndpointRegistry kafkaRegistry;
    private final ThreadPoolTaskExecutor httpExecutor;

    public GracefulShutdownConfig(
            KafkaListenerEndpointRegistry kafkaRegistry,
            @Qualifier("httpRequestExecutor") ThreadPoolTaskExecutor httpExecutor) {
        this.kafkaRegistry = kafkaRegistry;
        this.httpExecutor = httpExecutor;
    }

    @PreDestroy
    public void onShutdown() {
        log.info("🛑 Graceful shutdown started...");

        // 1. Kafka consumerlarni to'xtatish — yangi message qabul qilmaydi
        stopKafkaConsumers();

        // 2. HTTP thread poolni to'xtatish — in-flight requestlar tugashini kutish
        shutdownHttpExecutor();

        log.info("✅ Graceful shutdown completed");
    }

    private void stopKafkaConsumers() {
        log.info("Stopping Kafka consumers...");
        try {
            kafkaRegistry.getListenerContainers().forEach(container -> {
                log.info("Stopping Kafka listener: {}", container.getListenerId());
                container.stop();
            });
            log.info("All Kafka consumers stopped");
        } catch (Exception e) {
            log.error("Error stopping Kafka consumers: {}", e.getMessage(), e);
        }
    }

    private void shutdownHttpExecutor() {
        log.info("Shutting down HTTP executor. Active tasks: {}, Queue size: {}",
                httpExecutor.getActiveCount(),
                httpExecutor.getThreadPoolExecutor().getQueue().size());

        httpExecutor.getThreadPoolExecutor().shutdown();

        try {
            if (!httpExecutor.getThreadPoolExecutor().awaitTermination(25, TimeUnit.SECONDS)) {
                log.warn("⚠ HTTP executor did not terminate in 25s, forcing shutdown. " +
                        "Remaining active: {}", httpExecutor.getActiveCount());
                httpExecutor.getThreadPoolExecutor().shutdownNow();
            } else {
                log.info("HTTP executor terminated gracefully");
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown interrupted, forcing...");
            httpExecutor.getThreadPoolExecutor().shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
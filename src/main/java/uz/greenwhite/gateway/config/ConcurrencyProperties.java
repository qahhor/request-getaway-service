package uz.greenwhite.gateway.config;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "gateway.concurrency")
public class ConcurrencyProperties {

    /**
     * Minimum consumer threads (always running)
     */
    private int minConcurrency = 3;

    /**
     * Maximum consumer threads (hard cap)
     */
    private int maxConcurrency = 15;

    /**
     * How often to check lag and adjust concurrency (ms)
     */
    private long monitorIntervalMs = 10_000;

    /**
     * Lag thresholds for scaling decisions
     * When lag > scaleUpThreshold → increase threads
     * When lag < scaleDownThreshold → decrease threads
     */
    private long scaleUpThreshold = 50;
    private long scaleDownThreshold = 10;

    /**
     * How many threads to add/remove per scaling step
     */
    private int scaleStep = 2;

    /**
     * Cooldown period between scaling actions (ms)
     * Prevents rapid scaling oscillation
     */
    private long scaleCooldownMs = 30_000;

    /**
     * Number of Kafka partitions per topic
     * concurrency should never exceed this
     */
    private int topicPartitions = 10;

    @PostConstruct
    public void validate() {
        if (minConcurrency < 1) {
            throw new IllegalArgumentException("minConcurrency must be >= 1");
        }
        if (maxConcurrency < minConcurrency) {
            throw new IllegalArgumentException("maxConcurrency must be >= minConcurrency");
        }
        if (maxConcurrency > topicPartitions) {
            log.warn("maxConcurrency ({}) exceeds topicPartitions ({}). " +
                            "Effective concurrency will be limited to partition count.",
                    maxConcurrency, topicPartitions);
            maxConcurrency = topicPartitions;
        }
        if (scaleUpThreshold <= scaleDownThreshold) {
            throw new IllegalArgumentException("scaleUpThreshold must be > scaleDownThreshold");
        }

        log.info("Concurrency config: min={}, max={}, partitions={}, " +
                        "scaleUp>{}, scaleDown<{}, step={}, cooldown={}ms",
                minConcurrency, maxConcurrency, topicPartitions,
                scaleUpThreshold, scaleDownThreshold, scaleStep, scaleCooldownMs);
    }

    /**
     * Calculate desired concurrency based on current lag
     */
    public int calculateDesiredConcurrency(long currentLag) {
        if (currentLag <= scaleDownThreshold) {
            return minConcurrency;
        }

        if (currentLag > scaleUpThreshold) {
            // Linear scaling between min and max based on lag
            double ratio = Math.min(1.0,
                    (double) (currentLag - scaleUpThreshold) / (scaleUpThreshold * 3));
            int desired = minConcurrency + (int) Math.ceil(
                    (maxConcurrency - minConcurrency) * ratio);
            return Math.min(desired, maxConcurrency);
        }

        // Between scaleDown and scaleUp thresholds — keep current
        return -1; // Signal: no change needed
    }
}
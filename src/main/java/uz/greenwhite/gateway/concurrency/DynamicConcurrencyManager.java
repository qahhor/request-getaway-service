package uz.greenwhite.gateway.concurrency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;
import uz.greenwhite.gateway.config.ConcurrencyProperties;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
@RequiredArgsConstructor
public class DynamicConcurrencyManager {

    private final KafkaListenerEndpointRegistry registry;
    private final ConcurrencyProperties properties;

    /**
     * Har bir listener uchun alohida state
     */
    private final Map<String, AtomicInteger> concurrencyMap = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> lastScaleTimeMap = new ConcurrentHashMap<>();

    /**
     * Listener ID bo'yicha concurrency ni lag ga qarab moslashtirish.
     *
     * @param listenerId @KafkaListener dagi id qiymati
     * @param currentLag hozirgi consumer lag (barcha partition lar yig'indisi)
     */
    public void adjustConcurrency(String listenerId, long currentLag) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);

        if (container == null) {
            log.warn("Listener container not found: {}", listenerId);
            return;
        }

        if (!(container instanceof ConcurrentMessageListenerContainer<?, ?> concurrent)) {
            log.warn("Container is not ConcurrentMessageListenerContainer: {}", listenerId);
            return;
        }

        // Listener uchun state olish yoki yaratish
        AtomicInteger currentConcurrency = concurrencyMap.computeIfAbsent(
                listenerId, k -> new AtomicInteger(0));
        AtomicLong lastScaleTime = lastScaleTimeMap.computeIfAbsent(
                listenerId, k -> new AtomicLong(0));

        int actual = concurrent.getConcurrency();
        if (currentConcurrency.compareAndSet(0, actual)) {
            log.info("Initial concurrency detected for [{}]: {}", listenerId, actual);
        }

        int desired = properties.calculateDesiredConcurrency(currentLag);

        // -1 means no change needed (lag is in neutral zone)
        if (desired == -1) {
            log.debug("Lag {} is in neutral zone, no scaling needed for [{}]", currentLag, listenerId);
            return;
        }

        int current = currentConcurrency.get();

        // Already at desired level
        if (desired == current) {
            log.debug("Concurrency already at desired level {} for [{}]", current, listenerId);
            return;
        }

        // Cooldown check
        long now = System.currentTimeMillis();
        long lastScale = lastScaleTime.get();
        if (now - lastScale < properties.getScaleCooldownMs()) {
            log.debug("Scaling cooldown active for [{}]. Last scale: {}ms ago",
                    listenerId, now - lastScale);
            return;
        }

        // Step-based scaling
        int newConcurrency;
        if (desired > current) {
            newConcurrency = Math.min(current + properties.getScaleStep(), desired);
        } else {
            newConcurrency = Math.max(current - properties.getScaleStep(), desired);
        }

        // Bounds check
        newConcurrency = Math.max(properties.getMinConcurrency(), newConcurrency);
        newConcurrency = Math.min(properties.getMaxConcurrency(), newConcurrency);

        if (newConcurrency == current) {
            return;
        }

        // Apply new concurrency
        try {
            concurrent.setConcurrency(newConcurrency);

            // stop/start o'rniga — faqat keyingi rebalance da qo'llaniladi
            // Bu mavjud threadlarni to'xtatmaydi, faqat yangi concurrency ni belgilaydi
            if (newConcurrency > current) {
                // Scale UP — yangi threadlar qo'shish uchun restart kerak
                concurrent.stop();
                concurrent.start();
            }
            // Scale DOWN — stop/start qilmaymiz, keyingi rebalance da tushadi

            currentConcurrency.set(newConcurrency);
            lastScaleTime.set(now);

            String direction = newConcurrency > current ? "⬆ SCALED UP" : "⬇ SCALED DOWN";
            log.info("{} [{}]: {} → {} (lag: {})",
                    direction, listenerId, current, newConcurrency, currentLag);

        } catch (Exception e) {
            log.error("Failed to adjust concurrency for [{}]: {}", listenerId, e.getMessage(), e);
        }
    }

    /**
     * Ma'lum listener ning hozirgi concurrency qiymati
     */
    public int getCurrentConcurrency(String listenerId) {
        AtomicInteger val = concurrencyMap.get(listenerId);
        return val != null ? val.get() : 0;
    }

    /**
     * Ma'lum listener ning oxirgi scaling vaqti
     */
    public long getLastScaleTime(String listenerId) {
        AtomicLong val = lastScaleTimeMap.get(listenerId);
        return val != null ? val.get() : 0;
    }
}
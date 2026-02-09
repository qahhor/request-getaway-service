package uz.greenwhite.gateway.concurrency;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import uz.greenwhite.gateway.config.ConcurrencyProperties;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConcurrencyMonitorService {

    private final AdminClient adminClient;
    private final DynamicConcurrencyManager concurrencyManager;
    private final ConcurrencyProperties properties;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${gateway.kafka.topics.request-new}")
    private String requestNewTopic;

    @Value("${gateway.kafka.topics.request-response}")
    private String requestResponseTopic;

    /**
     * Listener ID lar — @KafkaListener(id = "...") bilan bir xil
     */
    public static final String REQUEST_LISTENER_ID = "requestConsumer";
    public static final String RESPONSE_LISTENER_ID = "responseConsumer";

    private final Map<String, Long> lagMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("ConcurrencyMonitor started: group={}, topics=[{}, {}], interval={}ms",
                consumerGroupId, requestNewTopic, requestResponseTopic,
                properties.getMonitorIntervalMs());
    }

    /**
     * Har N millisekundda barcha consumer lar uchun lag tekshirish
     */
    @Scheduled(fixedDelayString = "${gateway.concurrency.monitor-interval-ms:10000}")
    public void monitorAndScale() {
        try {
            // 1. Consumer group ning barcha committed offset larini bir marta olish
            Map<TopicPartition, OffsetAndMetadata> allCommittedOffsets = getCommittedOffsets();

            if (allCommittedOffsets == null || allCommittedOffsets.isEmpty()) {
                log.debug("No committed offsets found for group: {}", consumerGroupId);
                return;
            }

            // 2. Request topic lag → RequestConsumer scaling
            long requestLag = calculateLagForTopic(requestNewTopic, allCommittedOffsets);
            lagMap.put(requestNewTopic, requestLag);
            log.debug("Consumer lag [{}]: {} messages", requestNewTopic, requestLag);
            concurrencyManager.adjustConcurrency(REQUEST_LISTENER_ID, requestLag);

            // 3. Response topic lag → ResponseConsumer scaling
            long responseLag = calculateLagForTopic(requestResponseTopic, allCommittedOffsets);
            lagMap.put(requestResponseTopic, responseLag);
            log.debug("Consumer lag [{}]: {} messages", requestResponseTopic, responseLag);
            concurrencyManager.adjustConcurrency(RESPONSE_LISTENER_ID, responseLag);

        } catch (Exception e) {
            log.error("Error monitoring consumer lag: {}", e.getMessage(), e);
        }
    }

    /**
     * Consumer group ning committed offset larini olish (bir marta, barcha topic lar uchun)
     */
    private Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets() throws Exception {
        ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(consumerGroupId);
        return result.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
    }

    /**
     * Bitta topic uchun lag hisoblash
     */
    private long calculateLagForTopic(String topicName,
                                      Map<TopicPartition, OffsetAndMetadata> allCommittedOffsets) throws Exception {

        // 1. Shu topic ning committed offset larini filter qilish
        Map<TopicPartition, OffsetAndMetadata> topicOffsets = allCommittedOffsets.entrySet().stream()
                .filter(e -> e.getKey().topic().equals(topicName))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (topicOffsets.isEmpty()) {
            return 0;
        }

        // 2. End offset larini olish
        Map<TopicPartition, OffsetSpec> offsetSpecMap = topicOffsets.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                adminClient.listOffsets(offsetSpecMap).all().get(10, TimeUnit.SECONDS);

        // 3. Lag hisoblash
        long totalLag = 0;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicOffsets.entrySet()) {
            TopicPartition partition = entry.getKey();
            long committedOffset = entry.getValue().offset();

            ListOffsetsResult.ListOffsetsResultInfo endOffsetInfo = endOffsets.get(partition);
            if (endOffsetInfo != null) {
                long partitionLag = Math.max(0, endOffsetInfo.offset() - committedOffset);
                totalLag += partitionLag;
            }
        }

        return totalLag;
    }

    /**
     * Metrics uchun — topic bo'yicha oxirgi bilgan lag
     */
    public long getLastKnownLag(String topicName) {
        return lagMap.getOrDefault(topicName, 0L);
    }
}
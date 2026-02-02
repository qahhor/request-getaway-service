package uz.greenwhite.gateway.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${gateway.kafka.topics.request-new}")
    private String requestNewTopic;

    @Value("${gateway.kafka.topics.request-response}")
    private String requestResponseTopic;

    @Value("${gateway.kafka.topics.request-callback}")
    private String requestCallbackTopic;

    @Value("${gateway.kafka.topics.request-dlq}")
    private String requestDlqTopic;

    // ==================== TOPICS ====================

    @Bean
    public NewTopic requestNewTopic() {
        return TopicBuilder.name(requestNewTopic)
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic requestResponseTopic() {
        return TopicBuilder.name(requestResponseTopic)
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic requestCallbackTopic() {
        return TopicBuilder.name(requestCallbackTopic)
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic requestDlqTopic() {
        return TopicBuilder.name(requestDlqTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // ==================== CONSUMER FACTORY ====================

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "uz.greenwhite.gateway.model,uz.greenwhite.gateway.model.kafka");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
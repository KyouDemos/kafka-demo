package cn.ok.demos.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * cn.ok.demos.kafkademo.config
 *
 * @author kyou on 2019-04-04 19:43
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    String groupId;

    @Value("${kafka.topic.single}")
    String singleTopic;

    @Value("${kafka.topic.multi}")
    String multiTopic;

    /**
     * 确保Topic存在
     *
     * @return NewTopic
     */
    @Bean
    public NewTopic singlePartitionTopic() {
        return new NewTopic(singleTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic multiPartitionTopic() {
        return new NewTopic(multiTopic, 3, (short) 1);
    }

    @Bean
    public KafkaConsumer<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

}

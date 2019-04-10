package cn.ok.demos.kafkademo.component;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * cn.ok.demos.kafkademo.component
 *
 * @author kyou on 2019-04-04 20:21
 */
@Slf4j
@Component
public class ListenerForKafka {

    /**
     * 消息监听
     *
     * @param record 监听到的记录
     */
    @KafkaListener(topics = "${kafka.topic.single}")
    public void singlePartition(ConsumerRecord<?, ?> record) {
        log.info("Topic(single_partition) Receive msg: {}", record.toString());
    }

    /**
     * 消息监听
     *
     * @param record 监听到的记录
     */
    @KafkaListener(topics = "${kafka.topic.multi}")
    public void multiPartition(ConsumerRecord<?, ?> record) {
        log.info("Topic(multi_partition) Receive msg: {}", record.toString());
    }
}

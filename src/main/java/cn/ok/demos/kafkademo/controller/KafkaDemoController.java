package cn.ok.demos.kafkademo.controller;

import cn.ok.demos.kafkademo.component.ListenableFutureCallbackDemo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * cn.ok.demos.kafkademo.controller
 *
 * @author kyou on 2019-04-04 19:59
 */
@Slf4j
@RestController
public class KafkaDemoController {
    private final KafkaTemplate<String, String> sender;
    private final ListenableFutureCallbackDemo listenableFutureCallbackDemo;

    private final KafkaConsumer kafkaConsumer;

    @Autowired
    public KafkaDemoController(KafkaTemplate<String, String> sender, ListenableFutureCallbackDemo listenableFutureCallbackDemo, KafkaConsumer kafkaConsumer) {
        this.sender = sender;
        this.listenableFutureCallbackDemo = listenableFutureCallbackDemo;
        this.kafkaConsumer = kafkaConsumer;
    }

    @GetMapping("/{topic}/send/{msg}")
    public String send(@PathVariable("topic") String topic, @PathVariable("msg") String msg) {
        ListenableFuture<SendResult<String, String>> result = sender.send(topic, msg);
        result.addCallback(listenableFutureCallbackDemo);
        return msg;
    }

    /**
     * 遍历topic中的全部记录（加入筛选条件即可实现记录查找）
     *
     * @param topic topic
     * @return msg
     */
    @GetMapping("/recordsOfTopic/{topic}")
    public String recordsOfTopic(@PathVariable String topic) {

        // is topic exist?
        if (!kafkaConsumer.listTopics().containsKey(topic)) {
            String msg = String.format("Topic(%s) is not exist.", topic);
            log.info(msg);
            return msg;
        }

        // get topic's all partitions
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : (List<PartitionInfo>) kafkaConsumer.partitionsFor(topic)) {
            topicPartitions.add(new TopicPartition(topic, partitionInfo.partition()));
        }

        // assign to patitions of topic
        kafkaConsumer.assign(topicPartitions);

        // seek to beginning
        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        // traverling all records of topic
        for (ConsumerRecord<String, String> record : (Iterable<ConsumerRecord<String, String>>) kafkaConsumer.poll(Duration.ofSeconds(100))) {
            log.info(record.toString());
        }

        return "see log";
    }
}

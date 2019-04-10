package cn.ok.demos.kafkademo.component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * 向kafka发送消息后的回调函数
 *
 * @author kyou on 2019-04-09 08:46
 */
@Slf4j
@Component
public class ListenableFutureCallbackDemo implements ListenableFutureCallback<SendResult<String, String>> {
    @Override
    public void onFailure(Throwable ex) {
        log.error("Failed to send msg to Kafka. exception:\n{}", ex);
    }

    @Override
    public void onSuccess(SendResult<String, String> result) {
        log.debug("Send msg success. Detail: {}", result.toString());
    }
}

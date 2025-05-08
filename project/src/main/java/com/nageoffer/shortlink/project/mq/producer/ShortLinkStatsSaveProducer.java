package com.nageoffer.shortlink.project.mq.producer;

import jakarta.websocket.SendResult;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

/**
 * 短链接监控状态保存消息队列生产者
 */
@Component
@RequiredArgsConstructor
public class ShortLinkStatsSaveProducer {

    private final StringRedisTemplate stringRedisTemplate;
    // 创建StreamKey
    // XADD "short_link:stats-stream" * "New key" "New value"
//    @Value("${spring.data.redis.channel-topic.short-link-stats}")
//    private String topic;

    /**
     * 发送延迟消费短链接统计
     * producerMap: fullShortUrl, gid, statsRecord
     * XADD short_link:stats-stream * field1 value1 field2 value2...
     */
    public void send(Map<String, String> producerMap) {
        // 向redis Stream消息队列中添加消息
        stringRedisTemplate.opsForStream().add(SHORT_LINK_STATS_STREAM_TOPIC_KEY, producerMap);
    }
}

//@Slf4j
//@Component
//@RequiredArgsConstructor
//public class ShortLinkStatsSaveProducer {
//
//    private final RocketMQTemplate rocketMQTemplate;
//
//    @Value("${rocketmq.producer.topic}")
//    private String statsSaveTopic;
//
//    /**
//     * 发送延迟消费短链接统计
//     */
//    public void send(Map<String, String> producerMap) {
//        String keys = UUID.randomUUID().toString();
//        producerMap.put("keys", keys);
//        Message<Map<String, String>> build = MessageBuilder
//                .withPayload(producerMap)
//                .setHeader(MessageConst.PROPERTY_KEYS, keys)
//                .build();
//        SendResult sendResult;
//        try {
//            sendResult = rocketMQTemplate.syncSend(statsSaveTopic, build, 2000L);
//            log.info("[消息访问统计监控] 消息发送结果：{}，消息ID：{}，消息Keys：{}", sendResult.getSendStatus(), sendResult.getMsgId(), keys);
//        } catch (Throwable ex) {
//            log.error("[消息访问统计监控] 消息发送失败，消息体：{}", JSON.toJSONString(producerMap), ex);
//            // 自定义行为...
//        }
//    }
//}
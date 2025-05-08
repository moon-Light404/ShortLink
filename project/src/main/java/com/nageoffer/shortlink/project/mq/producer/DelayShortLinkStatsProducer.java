package com.nageoffer.shortlink.project.mq.producer;

import cn.hutool.core.lang.UUID;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RDelayedQueue;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.DELAY_QUEUE_STATS_KEY;

/**
 * 延迟消费短链接统计发送者
 */
@Component
@Deprecated
@RequiredArgsConstructor
public class DelayShortLinkStatsProducer {

    private final RedissonClient redissonClient;

    /**
     * 发送延迟消费短链接统计
     *
     * @param statsRecord 短链接统计实体参数
     */
    public void send(ShortLinkStatsRecordDTO statsRecord) {
        statsRecord.setKeys(UUID.fastUUID().toString()); // 使用UUID设置消息唯一标识id
        // 阻塞队列，实际存储消息的队列，消费者从此队列获取消息
        RBlockingDeque<ShortLinkStatsRecordDTO> blockingDeque = redissonClient.getBlockingDeque(DELAY_QUEUE_STATS_KEY);
        // 延迟队列，管理消息的延迟投递逻辑，到期后自动将消息转移到阻塞队列
        RDelayedQueue<ShortLinkStatsRecordDTO> delayedQueue = redissonClient.getDelayedQueue(blockingDeque);
        // 将统计记录放入延迟队列，设置5s延迟，到期后消息进入阻塞队列等待消费
        delayedQueue.offer(statsRecord, 5, TimeUnit.SECONDS); // 设置5s的延时
    }
}
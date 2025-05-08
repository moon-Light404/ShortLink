package com.nageoffer.shortlink.project.config;

import com.nageoffer.shortlink.project.mq.consumer.ShortLinkStatsSaveConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_GROUP_KEY;
import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.SHORT_LINK_STATS_STREAM_TOPIC_KEY;

/**
 * Redis Stream 消息队列配置
 */
@Configuration
@RequiredArgsConstructor
public class RedisStreamConfiguration {

    private final RedisConnectionFactory redisConnectionFactory;
    private final ShortLinkStatsSaveConsumer shortLinkStatsSaveConsumer;

    /**
     *
     * 功能：定义了一个线程池，用于异步处理 Redis Stream 消息。
     * 线程池配置：
     * 核心线程数为 CPU 核心数。
     * 最大线程数为核心线程数的 1.5 倍。
     * 空闲线程的存活时间为 60 秒。
     * 使用无界队列 (LinkedBlockingQueue)，确保所有任务都能被处理，不会因队列满而丢弃消息。
     * 自定义线程工厂，设置线程名称为 stream_consumer_short-link_stats_，并将线程设置为守护线程。
     * 目的：为 Redis Stream 消息消费提供高并发的异步处理能力。
     */
    // 这段代码定义了一个名为 asyncStreamConsumer 的
    // Spring Bean，用于创建一个线程池，专门处理 Redis Stream 消息的异步消费任务。
    @Bean
    public ExecutorService asyncStreamConsumer() {
        AtomicInteger index = new AtomicInteger();
        int processors = Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(processors,
                processors + processors >> 1,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), // 使用无界队列，此时最大线程数将失效||
                // 适合Stream消息消费（高并发），短时间高频率任务，确保所有消息都能处理，而不会因为队列满而丢弃消息
                runnable -> {
                    Thread thread = new Thread(runnable);
                    thread.setName("stream_consumer_short-link_stats_" + index.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }
        );
    }
    // Redis Stream实现监听和消费
    // 该线程池主要用于绑定到 Redis Stream 消息监听器中，处理从 Redis Stream 拉取到的消息，确保高效并发处理。
    @Bean(initMethod = "start", destroyMethod = "stop")
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer(ExecutorService asyncStreamConsumer) {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> options =
                StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                        .builder()
                        // 一次最多获取多少条消息
                        .batchSize(10)
                        // 执行从 Stream 拉取到消息的任务流程
                        .executor(asyncStreamConsumer) // 绑定了一个线程
                        // 如果没有拉取到消息，需要阻塞的时间。不能大于 ${spring.data.redis.timeout}，否则会超时
                        .pollTimeout(Duration.ofSeconds(3))
                        .build();
        StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer =
                StreamMessageListenerContainer.create(redisConnectionFactory, options);
        // 指定消费者组和消费者名称 || 指定监听的Stream，从最后消费的位置（ReadOffset.lastConsumed()）开始读取消息
        // StreamKey
        streamMessageListenerContainer.receiveAutoAck(Consumer.from(SHORT_LINK_STATS_STREAM_GROUP_KEY, "stats-consumer"),
                StreamOffset.create(SHORT_LINK_STATS_STREAM_TOPIC_KEY, ReadOffset.lastConsumed()), shortLinkStatsSaveConsumer);
        return streamMessageListenerContainer;
    }
}
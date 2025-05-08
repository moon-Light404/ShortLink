package com.nageoffer.shortlink.project.config;

import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
// @Configuration 注解用于标记一个类为 Spring 的配置类。
// 配置类是用来定义 Bean 的地方，Spring 会在启动时自动扫描并加载这些 Bean
@Configuration
public class RBloomFilterConfiguration {
    /**
     * 防止短链接创建查询数据库的布隆过滤器
     */
    @Bean
    public RBloomFilter<String> ShortUriCreateRegisterCachePenetrationBloomFilter(RedissonClient redissonClient) {
        RBloomFilter<String> cachePenetrationBloomFilter = redissonClient.getBloomFilter("shortUriCreateCachePenetrationBloomFilter");
        cachePenetrationBloomFilter.tryInit(100000000L, 0.001);
        return cachePenetrationBloomFilter;
    }
}

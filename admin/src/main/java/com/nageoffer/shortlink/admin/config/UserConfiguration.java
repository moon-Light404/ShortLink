package com.nageoffer.shortlink.admin.config;

import com.nageoffer.shortlink.admin.common.biz.user.UserFlowRiskControlFilter;
import com.nageoffer.shortlink.admin.common.biz.user.UserTransmitFilter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * 用户配置自动装配
 */
@Configuration
public class UserConfiguration {

    /**
     * 用户信息传递过滤器
     * 用户发起请求时候拦截请求，获取请求头中的用户信息，存入threadlocal
     */
    @Bean
    public FilterRegistrationBean<UserTransmitFilter> globalUserTransmitFilter() {
        FilterRegistrationBean<UserTransmitFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new UserTransmitFilter()); // 创建过滤器实例
        registration.addUrlPatterns("/*"); // 拦截所有请求
        registration.setOrder(0); // 设置优先级最高
        return registration;
    }

    /**
     * 用户操作流量风控过滤器
     * 作用：根据配置限制用户操作频率（如防刷接口）
     * 条件化注册：仅当配置项 short-link.flow-limit.enable=true 时生效。
     */
    @Bean
    @ConditionalOnProperty(name = "short-link.flow-limit.enable", havingValue = "true")
    public FilterRegistrationBean<UserFlowRiskControlFilter> globalUserFlowRiskControlFilter(
            StringRedisTemplate stringRedisTemplate,
            UserFlowRiskControlConfiguration userFlowRiskControlConfiguration) {
        FilterRegistrationBean<UserFlowRiskControlFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new UserFlowRiskControlFilter(stringRedisTemplate, userFlowRiskControlConfiguration));
        registration.addUrlPatterns("/*"); // 拦截所有请求
        registration.setOrder(10); // 优先级低于用户信息过滤器，在用户信息解析后执行
        return registration;
    }
}

package com.nageoffer.shortlink.admin.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 用户操作流量风控配置文件
 * 根据登录用户做出控制，比如 x 秒请求后管系统的频率最多 x 次。
 */
@Data
@Component
@ConfigurationProperties(prefix = "short-link.flow-limit")
public class UserFlowRiskControlConfiguration {

    /**
     * 是否开启用户流量风控验证
     */
    private Boolean enable;

    /**
     * 流量风控时间窗口，单位：秒
     */
    private String timeWindow;

    /**
     * 流量风控时间窗口内可访问次数
     */
    private Long maxAccessCount;
}
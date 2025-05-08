package com.nageoffer.shortlink.project.config;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 初始化限流配置
 * 实现InitializingBean接口，spring容器会在Bean属性注入完成后自动调用afterPropertiesSet方法
 * 执行一些操作：如数据校验、资源初始化、逻辑预处理
 */
@Component
public class SentinelRuleConfig implements InitializingBean {
    // bean初始化阶段执行自定义逻辑：bean属性注入完成后，bean正式投入使用前afterPropertiesSet被自动调用
    @Override
    public void afterPropertiesSet() throws Exception {
        List<FlowRule> rules = new ArrayList<>();
        FlowRule createOrderRule = new FlowRule();
        createOrderRule.setResource("create_short-link");
        createOrderRule.setGrade(RuleConstant.FLOW_GRADE_QPS); // // 限流阈值类型（QPS），其他的还有并发线程数
        createOrderRule.setCount(20);   // 阈值：每秒最多 1 次请求
        rules.add(createOrderRule);
        FlowRuleManager.loadRules(rules);
    }
}


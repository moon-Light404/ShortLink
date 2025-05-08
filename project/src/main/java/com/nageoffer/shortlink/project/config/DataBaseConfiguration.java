package com.nageoffer.shortlink.project.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataBaseConfiguration {

    /**
     * 分页插件
     * MyBatis-Plus 拦截 SQL 语句，根据 IPage 对象中的分页信息，自动添加 LIMIT 和 OFFSET 语句。
     * MyBatis-Plus 执行 SQL 语句，并将查询结果封装到 IPage 对象中。
     */
    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        interceptor.addInnerInterceptor(new PaginationInnerInterceptor(DbType.MYSQL));
        return interceptor;
    }
}
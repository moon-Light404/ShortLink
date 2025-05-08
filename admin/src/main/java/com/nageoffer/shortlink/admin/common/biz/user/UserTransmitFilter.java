package com.nageoffer.shortlink.admin.common.biz.user;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.google.common.collect.Lists;
import com.nageoffer.shortlink.admin.common.convention.exception.ClientException;
import com.nageoffer.shortlink.admin.common.convention.result.Results;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;

import static com.nageoffer.shortlink.admin.common.constant.RedisCacheConstant.USER_LOGIN_KEY;

/**
 * 用户信息传输过滤器
 * 根据用户请求的token设置UserContext中的用户信息，用户上下文
 * Spring 配置类，用于注册一个自定义的过滤器 UserTransmitFilter，并将其应用到所有的 URL 路径（/*）
 */
@RequiredArgsConstructor
public class UserTransmitFilter implements Filter {


    @SneakyThrows
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) {
        {
            HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
            String username = httpServletRequest.getHeader("username");
            // 如果username不空，设置用户上下文
            // 短链接服务请求没有username，直接放行
            if (StrUtil.isNotBlank(username)) {
                String userId = httpServletRequest.getHeader("userId");
                String realName = httpServletRequest.getHeader("realName");
                UserInfoDTO userInfoDTO = new UserInfoDTO(userId, username, realName);
                // 设置threadlocal
                UserContext.setUser(userInfoDTO);
            }
            try {
                filterChain.doFilter(servletRequest, servletResponse); // 处理其他请求
            } finally {
                UserContext.removeUser();
            }
        }
    }
}

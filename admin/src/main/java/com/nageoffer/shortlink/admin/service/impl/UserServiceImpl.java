package com.nageoffer.shortlink.admin.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.nageoffer.shortlink.admin.common.convention.exception.ClientException;
import com.nageoffer.shortlink.admin.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.admin.common.enums.UserErrorCodeEnum;
import com.nageoffer.shortlink.admin.dao.entity.UserDO;
import com.nageoffer.shortlink.admin.dao.mapper.UserMapper;
import com.nageoffer.shortlink.admin.dto.req.UserLoginReqDTO;
import com.nageoffer.shortlink.admin.dto.req.UserRegisterReqDTO;
import com.nageoffer.shortlink.admin.dto.req.UserUpdateReqDTO;
import com.nageoffer.shortlink.admin.dto.resp.UserLoginRespDTO;
import com.nageoffer.shortlink.admin.dto.resp.UserRespDTO;
import com.nageoffer.shortlink.admin.service.GroupService;
import com.nageoffer.shortlink.admin.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.BeanUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.nageoffer.shortlink.admin.common.constant.RedisCacheConstant.LOCK_USER_REGISTER_KEY;
import static com.nageoffer.shortlink.admin.common.constant.RedisCacheConstant.USER_LOGIN_KEY;
import static com.nageoffer.shortlink.admin.common.enums.UserErrorCodeEnum.USER_EXIST;
import static com.nageoffer.shortlink.admin.common.enums.UserErrorCodeEnum.USER_SAVE_ERROR;

/**
 * 用户接口实现层
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class UserServiceImpl extends ServiceImpl<UserMapper, UserDO> implements UserService {

    private final RBloomFilter<String> userRegisterCachePenetrationBloomFilter;
    private final RedissonClient redissonClient;
    private final StringRedisTemplate stringRedisTemplate;
    private final GroupService groupService;

    @Override
    public UserRespDTO getUserByUsername(String username) {
        LambdaQueryWrapper<UserDO> queryWrapper = Wrappers.lambdaQuery(UserDO.class).eq(UserDO::getUsername, username);
        UserDO userDO = baseMapper.selectOne(queryWrapper);
        if (userDO == null) {
            throw new ServiceException(UserErrorCodeEnum.USER_NULL);
        }
        UserRespDTO result = new UserRespDTO(); // 手机号脱敏了
        BeanUtils.copyProperties(userDO, result);
        return result;
    }

    @Override
    public Boolean hasUsername(String username) {
        return !userRegisterCachePenetrationBloomFilter.contains(username);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void register(UserRegisterReqDTO requestParam) {
        if(!hasUsername(requestParam.getUsername())) { // 布隆过滤器判断用户名已存在，避免多次查询数据库
            throw new ClientException(UserErrorCodeEnum.USER_NAME_EXIST);
        }
        // 一个用户名对应一个锁，防止多个相同的用户名并发注册
        // 防止恶意请求毫秒级触发大量请求去注册一个未注册的用户名(布隆过滤器查不到，直接查数据库)
        // 当多个线程同时注册同一个用户名时，锁可以保证只有一个线程插入数据库
        RLock lock = redissonClient.getLock(LOCK_USER_REGISTER_KEY + requestParam.getUsername());
        try {
            if(lock.tryLock()) { // 没有获取到锁，直接返回
                try {
                    int inserted = baseMapper.insert(BeanUtil.toBean(requestParam, UserDO.class));
                    if (inserted < 1) {
                        throw new ClientException(USER_SAVE_ERROR);
                    }
                } catch (DuplicateKeyException ex) {
                    throw new ClientException(USER_EXIST);
                }
                groupService.saveGroup(requestParam.getUsername(), "默认分组"); // 注册用户后默认创建对应分组
                userRegisterCachePenetrationBloomFilter.add(requestParam.getUsername()); // 注册成功后将用户名加入布隆过滤器
                return;
            }
            throw new ClientException(UserErrorCodeEnum.USER_NAME_EXIST);
        } finally {
            lock.unlock();
        }

    }

    /**
     * 修改用户
     * @param requestParam 请求参数
     */
    @Override
    public void update(UserUpdateReqDTO requestParam) {
        // TODO 验证当前用户名是否为当前登录用户
        LambdaUpdateWrapper<UserDO> updateWrapper = Wrappers.lambdaUpdate(UserDO.class).eq(UserDO::getUsername, requestParam.getUsername());
        baseMapper.update(BeanUtil.toBean(requestParam, UserDO.class),updateWrapper);
    }

    @Override
    public UserLoginRespDTO login(UserLoginReqDTO requestParam) {
        LambdaQueryWrapper<UserDO> queryWrapper = Wrappers.lambdaQuery(UserDO.class).eq(UserDO::getUsername, requestParam.getUsername())
                .eq(UserDO::getPassword, requestParam.getPassword())
                .eq(UserDO::getDelFlag, 0);
        UserDO userDO = baseMapper.selectOne(queryWrapper);
        if (userDO == null) {
            throw new ClientException("用户不存在");
        }

        // 下面这种有漏洞：每次登录接口发起请求都会返回一个token，如果攻击者获取到token，就可以伪造用户登录
//        String uuid = UUID.randomUUID().toString();
//        stringRedisTemplate.opsForValue().set(uuid, JSON.toJSONString(userDO), 30L, TimeUnit.MINUTES);
//        return new UserLoginRespDTO(uuid); // 返回uuid-token
        Map<Object ,Object> hasLoginMap = stringRedisTemplate.opsForHash().entries(USER_LOGIN_KEY + requestParam.getUsername()); //获取键值对
        if (CollUtil.isNotEmpty(hasLoginMap)) {
            // 防止token过期了用户请求返回401，定时任务请求login接口刷新token有效时间
            stringRedisTemplate.expire(USER_LOGIN_KEY + requestParam.getUsername(), 30L, TimeUnit.MINUTES);
            String token = hasLoginMap.keySet().stream()// 寻找key
                    .findFirst()
                    .map(Object::toString)
                    .orElseThrow(() -> new ClientException("用户登录错误"));
            return new UserLoginRespDTO(token);
        } // 用户重复登录，返回已登录的token
        /**
         * Hash
         *  key: login_用户名
         *  value:
         *      key: token(uuid)
         *      val: JSON字符串(用户信息)
         */
        // 第一次登录：返回token
        String uuid = UUID.randomUUID().toString();
        stringRedisTemplate.opsForHash().put(USER_LOGIN_KEY + requestParam.getUsername(), uuid, JSON.toJSONString(userDO));
        stringRedisTemplate.expire(USER_LOGIN_KEY + requestParam.getUsername(), 30L, TimeUnit.MINUTES);
        // 设置过期时间30min

        return new UserLoginRespDTO(uuid); // 返回uuid-token
    }

    @Override
    public Boolean checkLogin(String username ,String token) {
        return stringRedisTemplate.opsForHash().get(USER_LOGIN_KEY  + username, token) != null;
    }

    @Override
    public void logout(String username, String token) {
        if (checkLogin(username, token)) { // 先检查是否已登录
            // 退出登录、删除token
            stringRedisTemplate.delete(USER_LOGIN_KEY  + username);
            return;
        }
        throw new ClientException("用户Token不存在或未登录");
    }
}

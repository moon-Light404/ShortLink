package com.nageoffer.shortlink.project.mq.consumer;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.Week;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.shortlink.project.common.convention.exception.ServiceException;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessLogsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkBrowserStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkDeviceStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkLocaleStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkNetworkStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkOsStatsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkStatsTodayDO;
import com.nageoffer.shortlink.project.dao.entity.ShortLinkGotoDO;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessLogsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkAccessStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkBrowserStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkDeviceStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkLocaleStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkNetworkStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkOsStatsMapper;
import com.nageoffer.shortlink.project.dao.mapper.LinkStatsTodayMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkGotoMapper;
import com.nageoffer.shortlink.project.dao.mapper.ShortLinkMapper;
import com.nageoffer.shortlink.project.dto.biz.ShortLinkStatsRecordDTO;
import com.nageoffer.shortlink.project.mq.idempotent.MessageQueueIdempotentHandler;
import com.nageoffer.shortlink.project.mq.producer.DelayShortLinkStatsProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.nageoffer.shortlink.project.common.constant.RedisKeyConstant.LOCK_GID_UPDATE_KEY;
import static com.nageoffer.shortlink.project.common.constant.ShortLinkConstant.AMAP_REMOTE_URL;

/**
 * 短链接监控状态保存消息队列消费者
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ShortLinkStatsSaveConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    private final ShortLinkMapper shortLinkMapper;
    private final ShortLinkGotoMapper shortLinkGotoMapper;
    private final RedissonClient redissonClient;
    private final LinkAccessStatsMapper linkAccessStatsMapper;
    private final LinkLocaleStatsMapper linkLocaleStatsMapper;
    private final LinkOsStatsMapper linkOsStatsMapper;
    private final LinkBrowserStatsMapper linkBrowserStatsMapper;
    private final LinkAccessLogsMapper linkAccessLogsMapper;
    private final LinkDeviceStatsMapper linkDeviceStatsMapper;
    private final LinkNetworkStatsMapper linkNetworkStatsMapper;
    private final LinkStatsTodayMapper linkStatsTodayMapper;
    private final DelayShortLinkStatsProducer delayShortLinkStatsProducer;
    private final StringRedisTemplate stringRedisTemplate;
    private final MessageQueueIdempotentHandler messageQueueIdempotentHandler;


    @Value("${short-link.stats.locale.amap-key}")
    private String statsLocaleAmapKey;
    // Redis Stream消息监听器，处理短链接统计数据的信息
    // OnMessage是Spring Data Redis 的 StreamListener 回调方法，当 Stream 中有新消息时会触发。
    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String stream = message.getStream(); // 获取消息的 Stream
        RecordId id = message.getId(); // 获取消息的 ID (redis-stream的消息ID)
        // 判断消息已经被消费
        if (messageQueueIdempotentHandler.isMessageProcessed(id.toString())) {
            // 判断当前的这个消息流程是否执行完成，因为有可能消费执行失败，但是已经加入了缓存，就进不来了，
            // 进一步判断消费是否执行完成
            if (messageQueueIdempotentHandler.isAccomplish(id.toString())) {
                return;
            }
            throw new ServiceException("消息未完成流程，需要消息队列重试");
        } // 消息没有消费完成会一直重试，然后redis中的消息标识过期，就开始执行下面
        try {
            Map<String, String> producerMap = message.getValue(); // 获取消息的内容，就是我们需要统计的消息体
            String fullShortUrl = producerMap.get("fullShortUrl");
            if (StrUtil.isNotBlank(fullShortUrl)) { // fullShortUrl 不为空，防御性编程，正常其实可以不写，因为请求一定有fullShortUrl
                String gid = producerMap.get("gid");
                ShortLinkStatsRecordDTO statsRecord = JSON.parseObject(producerMap.get("statsRecord"), ShortLinkStatsRecordDTO.class);
                actualSaveShortLinkStats(fullShortUrl, gid, statsRecord);
            }
            // 从指定的Stream中删除消息
            stringRedisTemplate.opsForStream().delete(Objects.requireNonNull(stream), id.getValue());
        } catch (Throwable ex) {
            // 某某某情况宕机了，要删除幂等标识，重新消费(因为没消费完成)
            messageQueueIdempotentHandler.delMessageProcessed(id.toString());
            log.error("记录短链接监控消费异常", ex);
        }
        // 设置消息已经被消费完1
        messageQueueIdempotentHandler.setAccomplish(id.toString());
    }
    //延迟队列处理：
    //延迟一段时间后再次尝试处理
    //如果仍无法获取锁，可能会再次延迟
    //直到成功获取锁并完成统计，将锁竞争的问题转移到延迟队列处理，不影响主消息流

    public void actualSaveShortLinkStats(String fullShortUrl, String gid, ShortLinkStatsRecordDTO statsRecord) {
        fullShortUrl = Optional.ofNullable(fullShortUrl).orElse(statsRecord.getFullShortUrl());
        RReadWriteLock readWriteLock = redissonClient.getReadWriteLock(String.format(LOCK_GID_UPDATE_KEY, fullShortUrl));
        RLock rLock = readWriteLock.readLock(); // 加入读锁，访问短链接的时候不能修改短链接的gid，而仍然被其他用户访问
        // 如果没有获取到读锁，通过延迟队列实现重试机制，确保统计信息最终一定会处理
        if (!rLock.tryLock()) { // 为了防止等待时间过长，直接发送到延迟队列处理，避免阻塞||使用trylock方法，如果获取不到锁，直接发送到延迟队列处理
            delayShortLinkStatsProducer.send(statsRecord);
            return;
        }
        // 当没有线程在修改gid的时候，获取读锁，就开始进行统计信息
        try {
            if (StrUtil.isBlank(gid)) { // 用fullShortUrl根据路由表查询gid
                LambdaQueryWrapper<ShortLinkGotoDO> queryWrapper = Wrappers.lambdaQuery(ShortLinkGotoDO.class)
                        .eq(ShortLinkGotoDO::getFullShortUrl, fullShortUrl);
                ShortLinkGotoDO shortLinkGotoDO = shortLinkGotoMapper.selectOne(queryWrapper);
                gid = shortLinkGotoDO.getGid();
            }
            int hour = DateUtil.hour(new Date(), true);
            Week week = DateUtil.dayOfWeekEnum(new Date());
            int weekValue = week.getIso8601Value();
            LinkAccessStatsDO linkAccessStatsDO = LinkAccessStatsDO.builder()
                    .pv(1)
                    .uv(statsRecord.getUvFirstFlag() ? 1 : 0)
                    .uip(statsRecord.getUipFirstFlag() ? 1 : 0)
                    .hour(hour)
                    .weekday(weekValue)
                    .fullShortUrl(fullShortUrl)
                    .gid(gid)
                    .date(new Date())
                    .build();
            linkAccessStatsMapper.shortLinkStats(linkAccessStatsDO); // 更新t_link_access_stats
            Map<String, Object> localeParamMap = new HashMap<>();
            localeParamMap.put("key", statsLocaleAmapKey);
            localeParamMap.put("ip", statsRecord.getRemoteAddr());
            String localeResultStr = HttpUtil.get(AMAP_REMOTE_URL, localeParamMap);
            JSONObject localeResultObj = JSON.parseObject(localeResultStr);
            String infoCode = localeResultObj.getString("infocode");
            String actualProvince = "未知";
            String actualCity = "未知";
            if (StrUtil.isNotBlank(infoCode) && StrUtil.equals(infoCode, "10000")) {
                String province = localeResultObj.getString("province"); // 获取地理位置
                boolean unknownFlag = StrUtil.equals(province, "[]"); // 表示没查到位置==未知
                LinkLocaleStatsDO linkLocaleStatsDO = LinkLocaleStatsDO.builder()
                        .province(actualProvince = unknownFlag ? actualProvince : province)
                        .city(actualCity = unknownFlag ? actualCity : localeResultObj.getString("city"))
                        .adcode(unknownFlag ? "未知" : localeResultObj.getString("adcode"))
                        .cnt(1)
                        .fullShortUrl(fullShortUrl)
                        .country("中国")
                        .gid(gid)
                        .date(new Date())
                        .build();
                linkLocaleStatsMapper.shortLinkLocaleState(linkLocaleStatsDO);
            }
            LinkOsStatsDO linkOsStatsDO = LinkOsStatsDO.builder()
                    .os(statsRecord.getOs())
                    .cnt(1)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkOsStatsMapper.shortLinkOsState(linkOsStatsDO);
            LinkBrowserStatsDO linkBrowserStatsDO = LinkBrowserStatsDO.builder()
                    .browser(statsRecord.getBrowser())
                    .cnt(1)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkBrowserStatsMapper.shortLinkBrowserState(linkBrowserStatsDO);
            LinkDeviceStatsDO linkDeviceStatsDO = LinkDeviceStatsDO.builder()
                    .device(statsRecord.getDevice())
                    .cnt(1)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkDeviceStatsMapper.shortLinkDeviceState(linkDeviceStatsDO);
            LinkNetworkStatsDO linkNetworkStatsDO = LinkNetworkStatsDO.builder()
                    .network(statsRecord.getNetwork())
                    .cnt(1)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkNetworkStatsMapper.shortLinkNetworkState(linkNetworkStatsDO);
            LinkAccessLogsDO linkAccessLogsDO = LinkAccessLogsDO.builder()
                    .user(statsRecord.getUv()) // 存入用户的cookie
                    .ip(statsRecord.getRemoteAddr())
                    .browser(statsRecord.getBrowser())
                    .os(statsRecord.getOs())
                    .network(statsRecord.getNetwork())
                    .device(statsRecord.getDevice())
                    .locale(StrUtil.join("-", "中国", actualProvince, actualCity))
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .build();
            linkAccessLogsMapper.insert(linkAccessLogsDO);
            shortLinkMapper.incrementStats(gid, fullShortUrl, 1, statsRecord.getUvFirstFlag() ? 1 : 0, statsRecord.getUipFirstFlag() ? 1 : 0);
            LinkStatsTodayDO linkStatsTodayDO = LinkStatsTodayDO.builder()
                    .todayPv(1)
                    .todayUv(statsRecord.getUvFirstFlag() ? 1 : 0)
                    .todayUip(statsRecord.getUipFirstFlag() ? 1 : 0)
                    .gid(gid)
                    .fullShortUrl(fullShortUrl)
                    .date(new Date())
                    .build();
            linkStatsTodayMapper.shortLinkTodayState(linkStatsTodayDO);
        } catch (Throwable ex) {
            log.error("短链接访问量统计异常", ex);
        } finally {
            rLock.unlock();
        }
    }
}


// RocketMQ

package com.nageoffer.shortlink.project.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessLogsDO;
import com.nageoffer.shortlink.project.dao.entity.LinkAccessStatsDO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkGroupStatsReqDTO;
import com.nageoffer.shortlink.project.dto.req.ShortLinkStatsReqDTO;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 访问日志监控持久层
 */
public interface LinkAccessLogsMapper extends BaseMapper<LinkAccessLogsDO> {
    /**
     * 根据短链接获取指定日期内高频访问IP数据
     */
    @Select("SELECT " +
            "    ip, " +
            "    COUNT(ip) AS count " +
            "FROM " +
            "    t_link_access_logs " +
            "WHERE " +
            "    full_short_url = #{param.fullShortUrl} " +
            "    AND gid = #{param.gid} " +
            "    AND create_time BETWEEN CONCAT(#{param.startDate},' 00:00:00') and CONCAT(#{param.endDate},' 23:59:59') " +
            "GROUP BY " +
            "    full_short_url, gid, ip " +
            "ORDER BY " +
            "    count DESC " +
            "LIMIT 5;")
    List<HashMap<String, Object>> listTopIpByShortLink(@Param("param") ShortLinkStatsReqDTO requestParam);

    /**
     * 根据短链接获取指定日期内新旧访客数据
     */
    @Select("SELECT " +
            "    SUM(old_user) AS oldUserCnt, " +
            "    SUM(new_user) AS newUserCnt " +
            "FROM ( " +
            "    SELECT " +  // 某个用户在该日期中访问次数大于1，则认为是老用户，否则是新用户
            "        CASE WHEN COUNT(DISTINCT DATE(create_time)) > 1 THEN 1 ELSE 0 END AS old_user, " +
            "        CASE WHEN COUNT(DISTINCT DATE(create_time)) = 1 AND MAX(create_time) >= #{param.startDate} AND MAX(create_time) <= #{param.endDate} THEN 1 ELSE 0 END AS new_user " +
            "    FROM " +
            "        t_link_access_logs " +
            "    WHERE " +
            "        full_short_url = #{param.fullShortUrl} " +
            "        AND gid = #{param.gid} " +
            "    GROUP BY " +
            "        user " + // 按照用户进行分组，避免同一个用户计算多次，然后计算新用户和老用户的数量
            ") AS user_counts;")
    HashMap<String, Object> findUvTypeCntByShortLink(@Param("param") ShortLinkStatsReqDTO requestParam);


    /**
     * 获取用户信息是否新老访客
     */
    @Select("<script> " +
            "SELECT " +
            "    user, " +
            "    CASE " +
            "        WHEN MIN(create_time) BETWEEN CONCAT(#{startDate},' 00:00:00') AND CONCAT(#{endDate},' 23:59:59') THEN '新访客' " +
            "        ELSE '老访客' " + // 首次访问时间再指定日期内，则为新访客，否则为老访客
            "    END AS uvType " +
            "FROM " +
            "    t_link_access_logs " +
            "WHERE " +
            "    full_short_url = #{fullShortUrl} " +
            "    AND gid = #{gid} " +
            "    AND user IN " +
            "    <foreach item='item' index='index' collection='userAccessLogsList' open='(' separator=',' close=')'> " +
            "        #{item} " +
            "    </foreach> " +
            "GROUP BY " +
            "    user;" + // 按照用户分组，每个用户有多条访问记录，这里确保每个用户只返回一条记录
            "    </script>"
    )
    List<Map<String, Object>> selectUvTypeByUsers(
            @Param("gid") String gid,
            @Param("fullShortUrl") String fullShortUrl,
            @Param("startDate") String startDate,
            @Param("endDate") String endDate,
            @Param("userAccessLogsList") List<String> userAccessLogsList
    );

    /**
     * 根据短链接获取指定日期内PV、UV、UIP数据
     */
    @Select("SELECT " +
            "    COUNT(user) AS pv, " +
            "    COUNT(DISTINCT user) AS uv, " +
            "    COUNT(DISTINCT ip) AS uip " +
            "FROM " +
            "    t_link_access_logs " +
            "WHERE " +
            "    full_short_url = #{param.fullShortUrl} " +
            "    AND gid = #{param.gid} " +
            "    AND create_time BETWEEN #{param.startDate} and #{param.endDate} " +
            "GROUP BY " +
            "    full_short_url, gid;")
    LinkAccessStatsDO findPvUvUidStatsByShortLink(@Param("param") ShortLinkStatsReqDTO requestParam);

    /**
     * 根据分组获取指定日期内高频访问IP数据
     */
    @Select("SELECT " +
            "    ip, " +
            "    COUNT(ip) AS count " +
            "FROM " +
            "    t_link_access_logs " +
            "WHERE " +
            "    gid = #{param.gid} " +
            "    AND create_time BETWEEN #{param.startDate} and #{param.endDate} " +
            "GROUP BY " +
            "    gid, ip " +
            "ORDER BY " +
            "    count DESC " +
            "LIMIT 5;")
    List<HashMap<String, Object>> listTopIpByGroup(@Param("param") ShortLinkGroupStatsReqDTO requestParam);

    /**
     * 根据分组获取指定日期内PV、UV、UIP数据
     */
    @Select("SELECT " +
            "    COUNT(user) AS pv, " +
            "    COUNT(DISTINCT user) AS uv, " +
            "    COUNT(DISTINCT ip) AS uip " +
            "FROM " +
            "    t_link_access_logs " +
            "WHERE " +
            "    gid = #{param.gid} " +
            "    AND create_time BETWEEN #{param.startDate} and #{param.endDate} " +
            "GROUP BY " +
            "    gid;")
    LinkAccessStatsDO findPvUvUidStatsByGroup(@Param("param") ShortLinkGroupStatsReqDTO requestParam);


    /**
     * 获取分组用户信息是否新老访客
     */
    @Select("<script> " +
            "SELECT " +
            "    user, " +
            "    CASE " + // 最早访问时间在这段范围内：新访客(之前没有访问过)，否则为老访客
            "        WHEN MIN(create_time) BETWEEN #{startDate} AND #{endDate} THEN '新访客' " +
            "        ELSE '老访客' " +
            "    END AS uvType " + // 作为uvType返回
            "FROM " +
            "    t_link_access_logs " +
            "WHERE " +
            "    gid = #{gid} " +
            "    AND user IN " +
            "    <foreach item='item' index='index' collection='userAccessLogsList' open='(' separator=',' close=')'> " +
            "        #{item} " +
            "    </foreach> " +
            "GROUP BY " +
            "    user;" + // 按照用户分组，这样每个用户只有一条记录
            "    </script>"
    )
    List<Map<String, Object>> selectGroupUvTypeByUsers(
            @Param("gid") String gid,
            @Param("startDate") String startDate,
            @Param("endDate") String endDate,
            @Param("userAccessLogsList") List<String> userAccessLogsList
    );
}
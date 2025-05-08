package com.nageoffer.shortlink.admin.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@TableName("t_group_unique")
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GroupUniqueDO {

    /**
     * id
     */
    private Long id;

    /**
     * 分组标识
     */
    private String gid;
}
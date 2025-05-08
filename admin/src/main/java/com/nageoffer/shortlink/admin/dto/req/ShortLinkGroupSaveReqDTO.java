package com.nageoffer.shortlink.admin.dto.req;

import lombok.Data;

/**
 * 短链接分组创建请求参数
 */
@Data
public class ShortLinkGroupSaveReqDTO {
    /**
     * 分组名称
     */
    private String name;
}

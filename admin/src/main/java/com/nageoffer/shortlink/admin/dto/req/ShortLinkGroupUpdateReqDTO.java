package com.nageoffer.shortlink.admin.dto.req;

import lombok.Data;

/**
 * 短链接分组更新请求参数
 */
@Data
public class ShortLinkGroupUpdateReqDTO {
    /**
     * 分组标识
     */
    private String gid;
    /**
     * 分组名
     */
    private String name;
}

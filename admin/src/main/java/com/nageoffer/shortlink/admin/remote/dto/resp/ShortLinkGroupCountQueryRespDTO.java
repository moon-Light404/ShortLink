package com.nageoffer.shortlink.admin.remote.dto.resp;

import lombok.Data;

@Data
public class ShortLinkGroupCountQueryRespDTO {
    /**
     * 分组标识
     */
    private String gid;

    /**
     * 短链接数量
     */
    private Integer shortLinkCount;
}

package com.nageoffer.shortlink.admin.common.serialize;

import cn.hutool.core.util.DesensitizedUtil;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * 手机号脱敏反序列化
 * 继承自 Jackson 的 JsonSerializer，专用于处理 String 类型的序列化
 * 重写Serialize方法（phone, jsonGenerator:Jackson 的 JSON 生成器，用于写入脱敏后的值。
 * serializerProvider：提供序列化上下文（如其他序列化器、配置）。）
 */
public class PhoneDesensitizationSerializer extends JsonSerializer<String> {

    @Override
    public void serialize(String phone, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        // DesensitizedUtil.mobilePhone(phone) 对手机号进行脱敏
        String phoneDesensitization = DesensitizedUtil.mobilePhone(phone);
        // 将脱敏后的字符串（如 138****5678）写入 JSON
        jsonGenerator.writeString(phoneDesensitization);
        // 只需要再字段上添加@JsonSerialize注解，自动脱敏
    }
}

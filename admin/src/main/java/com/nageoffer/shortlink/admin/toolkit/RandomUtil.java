package com.nageoffer.shortlink.admin.toolkit;

import java.util.Random;

public class RandomUtil {

    // 定义包含数字和英文字母的字符池
    private static final String CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    // 随机数生成器
    private static final Random random = new Random();

    /**
     * 生成包含数字和英文字母的 6 位随机数
     *
     * @return 6 位随机数
     */
    public static String generateRandomCode() {
        StringBuilder sb = new StringBuilder(6);
        for (int i = 0; i < 6; i++) {
            // 从字符池中随机选择一个字符
            int index = random.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(index));
        }
        return sb.toString();
    }


}
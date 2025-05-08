package com.nageoffer.shortlink.project.service.impl;

import com.nageoffer.shortlink.project.service.UrlTitleService;
import lombok.SneakyThrows;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.stereotype.Service;

import java.net.HttpURLConnection;
import java.net.URL;

/**
 * URL 标题接口实现层
 */
@Service // Lombok 提供的注解，用于自动抛出受检异常（如 IOException、MalformedURLException），避免显式编写 try-catch 块。
public class UrlTitleServiceImpl implements UrlTitleService {

    @SneakyThrows
    @Override
    public String getTitleByUrl(String url) {
        URL targetUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) targetUrl.openConnection();
        connection.setRequestMethod("GET");
        connection.connect();
        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {
            // 使用 Jsoup 解析 HTML 文档以提取标题
            Document document = Jsoup.connect(url).get();
            return document.title();
        }
        return "Error while fetching title.";
    }
}
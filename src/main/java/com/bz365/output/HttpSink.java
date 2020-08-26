package com.bz365.output;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.http.Consts;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;

/**
 * @author :LJF
 * @date :2020/2/15 12:20
 * @description:
 * @version: 1.0
 */
public class HttpSink<T> extends RichSinkFunction<T> {
    private CloseableHttpClient httpClient;
    private HttpPost httpPost;
    private URI uri;
    private String json;

    public HttpSink(URI uri, String json) {
        this.uri = uri;
        this.json = json;
    }

    @Override   //说明了被标注的方法重载了父类的方法，起到了断言的作用。可以避免方法名跟参数写错。
    public void open(Configuration parameters){
        httpClient = HttpClients.createDefault();
        httpPost = new HttpPost(uri);
    }

    @Override
    public void invoke(T value, Context context) throws SQLException, IOException {
        StringEntity entity = new StringEntity(json, Consts.UTF_8);
        entity.setContentEncoding("UTF-8");
        entity.setContentType("application/json");
        httpPost.setEntity(entity);
        try {
            httpClient.execute(httpPost);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(){
        if (httpClient != null){
            try {
                httpClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

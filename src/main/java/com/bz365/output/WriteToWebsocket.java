package com.bz365.output;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.java_websocket.enums.ReadyState;

/**
 * @author : LJF
 * @date : 2020/4/1 17:16
 * @description: 使用websocket的方式将数据发送到web后台
 * @version: 1.0
 */
public class WriteToWebsocket extends RichSinkFunction<JSONObject> {    //关键字extends继承一个已有的类：RichSinkFunction<JSONObject>
    static MyWebSocketClient client = null;
    String url;

    public WriteToWebsocket(String url) {
        this.url = url;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        client = new MyWebSocketClient(url);
        client.connect();                   //连接
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        client.send(value.toString());      //发送
    }

    @Override
    public void close() throws Exception {
        client.close();                     //关闭
    }

    // 用来主动将屏幕上的数值置0
    public static void clearView(String str){
        client.send(str);
    }
}

package com.bz365.output;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

/**
 * @author : LJF
 * @date : 2020/4/1 15:04
 * @description: 用于创建websocket客户端
 * @version: 1.0
 */
// @Deprecated注解：若某类或某方法加上该注解之后，表示此方法或类不再建议使用，调用时也会出现删除线，但并不代表不能用，
// 只是说，不推荐使用，因为还有更好的方法可以调用。
public class MyWebSocketClient extends WebSocketClient {
    public MyWebSocketClient(String url) throws URISyntaxException {
        super(new URI(url));
    }


    @Override
    public void onOpen(ServerHandshake shake) {
        System.out.println("握手...");
        for(Iterator<String> it = shake.iterateHttpFields(); it.hasNext();) {
            String key = it.next();
            System.out.println(key+":"+shake.getFieldValue(key));
        }
    }

    @Override
    public void onMessage(String paramString) {
        System.out.println("接收到消息:"+paramString);
    }

    @Override
    public void onClose(int paramInt, String paramString, boolean paramBoolean) {
        System.out.println("关闭..." + System.currentTimeMillis());
        System.out.println("websocket错误参数" + paramInt);
    }

    @Override
    public void onError(Exception e) {
        System.out.println("异常"+e);
    }
}

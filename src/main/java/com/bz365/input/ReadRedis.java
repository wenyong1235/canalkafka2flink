package com.bz365.input;

import com.bz365.utils.ConfigUtil;
import com.bz365.utils.RedisUtil;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author : LJF
 * @date : 2020/4/23 14:34
 * @description:
 * @version: 1.0
 */
public class ReadRedis {
    private static Jedis jedis = null;

    public static HashMap readRedis(){          //hashmap散列表k-v
        jedis = RedisUtil.getJedisInstance();
        Map<String, String> map = jedis.hgetAll(ConfigUtil.redisDXCanalCount);      //创建hashmap对象：map（键为string-值也为string）
        return (HashMap) map;                   //返回对象map
    }
}

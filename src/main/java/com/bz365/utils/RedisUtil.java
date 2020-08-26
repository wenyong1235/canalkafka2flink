package com.bz365.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author : LJF
 * @date : 2020/4/23 14:34
 * @description:
 * @version: 1.0
 */
public class RedisUtil {
    private static String HOST = ConfigUtil.redisHost;
    private static int PORT = Integer.valueOf(ConfigUtil.redisPort);
    private static JedisPoolConfig poolConfig = null;
    private static volatile JedisPool jedisPool = null;


    private static JedisPool getJedisPoolInstance(){
        if (jedisPool == null) {
            synchronized (RedisUtil.class) {
                if (jedisPool == null) {
                    poolConfig = new JedisPoolConfig();
                    poolConfig.setMaxTotal(25);
                    poolConfig.setMaxIdle(10);
                    poolConfig.setMinIdle(5);
                    poolConfig.setTestOnBorrow(true);

                    jedisPool = new JedisPool(poolConfig, HOST, PORT);
                }
            }
        }
        return jedisPool;
    }

    /**
     * 返回jedis实例对象
     * @return
     */
    public static Jedis getJedisInstance(){
        return getJedisPoolInstance().getResource();
    }

}

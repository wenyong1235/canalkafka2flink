package com.bz365.output;

import com.bz365.utils.RedisUtil;
import com.bz365.utils.TimeUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : LJF
 * @date : 2020/4/22 14:57
 * @description: 将数据临时写到redis保存,从redis去读保存的数据
 *                 暂存到redis里的K-V数据格式为 flume-dx-logCount  20200501:43534543
 * @version: 1.0
 */
public class WriteRedis extends RichSinkFunction<Map<String, String>> {
    // redis里保存的数据格式 K-V,K为 "canal-dx-canalCount" V 是一个HashMap里面有 date、dx-bz_user、dx-bz_order、dx-bz_ware、dx-bz_cms几个字段,
    // date用于标识当前的map数是哪天的其他几个是每个库处理的数据量
    private Jedis jedis = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = RedisUtil.getJedisInstance();       //？初始化？
    }

    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        // 数据流的hashmap里是没有date字段的，需要在此处拼接上
        value.put("date",TimeUtil.sqlDate().toString());
        jedis.hmset("canal-dx-canalCount",value);       //同时将多个 field-value (域-值)对设置到哈希表 key 中。
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}

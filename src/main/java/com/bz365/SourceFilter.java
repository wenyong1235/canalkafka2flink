package com.bz365;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import static com.bz365.utils.JsonUtil.isJsonObject;
import static com.bz365.utils.TimeUtil.getDateTime;

/**
 * @author :LJF
 * @date :2020/2/13 15:10
 * @description: 源文件
 * @version: 1.0
 */
public class SourceFilter {
    public static DataStream<Tuple5<String, String, String,String,String>> sourceFilter(DataStream<String> value){
        SingleOutputStreamOperator<String> policyUserJson = value.filter(line -> isJsonObject(line));
        DataStream<String> policyUserUpdate = policyUserJson.filter(line -> !(JSON.parseObject(line).getString("type").equalsIgnoreCase("DELETE")));
        DataStream<String> policyUserMobile = policyUserUpdate.filter(line -> filter(line));

        // 一个数据流内符合要求的所有数据拼接成一个大的字符串,而且使用reduce()避免了使用process()导致结果累积的问题
        SingleOutputStreamOperator<String> policyUserMobile1 = policyUserMobile.timeWindowAll(Time.seconds(30)).reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2){
                return value1 + "&" + value2;
            }
        });


        DataStream<Tuple5<String, String, String,String,String>> flatMap = policyUserMobile1.flatMap(new FlatMapFunction<String, Tuple5<String, String,String,String,String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple5<String, String, String,String,String>> out) throws Exception {
                // order_id、policy_id、user_id、tmobile
                String[] splits = value.split("&");
                for (String str:splits) {
                    JSONObject jsonObject = JSON.parseObject(str);
                    JSONObject datas = jsonObject.getJSONArray("data").getJSONObject(0);
                    String orderID = datas.getString("order_id");
                    String userID = datas.getString("user_id");
                    String policyID = datas.getString("policy_id");
                    // 截取手机号前7位
                    String tmobile = datas.getString("tmobile").substring(0, 7);
                    // 拼接数据时要加上时间信息
                    out.collect(new Tuple5<>(orderID, userID, policyID, tmobile,getDateTime()));
                }
            }
        });
        return flatMap;
    }

    private static boolean filter(String line){
        // 过滤出 order_id、policy_id 同时都不为空的数据
        JSONObject data = JSON.parseObject(line).getJSONArray("data").getJSONObject(0); //判断是json格式的数据
        String order_id = data.getString("order_id");
        String policy_id = data.getString("policy_id");
        String tmobile = data.getString("tmobile");
        return StringUtils.isNotEmpty(order_id) && StringUtils.isNotEmpty(policy_id) && (tmobile.length()==11);
    }
}

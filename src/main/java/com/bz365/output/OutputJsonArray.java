package com.bz365.output;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.bz365.utils.ConfigUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.util.Iterator;

/**
 * @author :LJF
 * @date :2020/2/20 17:26
 * @description:
 * @version: 1.0
 */
public class OutputJsonArray {
    public static SingleOutputStreamOperator<JSONObject> outPutJsonArray(DataStream<Row> dataStream) {
        JSONObject jsonObject = new JSONObject();       //创建json对象
        JSONObject tmp = new JSONObject();
        JSONArray jsonArray = new JSONArray();

        SingleOutputStreamOperator<JSONObject> reduce = dataStream.map(new MapFunction<Row, JSONObject>() {
            @Override
            public JSONObject map(Row value) throws Exception {         //put各种信息
                jsonObject.put("orderID", value.getField(0).toString());
                jsonObject.put("policyID", value.getField(1).toString());
                jsonObject.put("province", value.getField(2).toString());
                jsonObject.put("city", value.getField(3).toString());
                jsonObject.put("create_time", value.getField(4).toString());
                // 解决FastJson循环引用的问题
                String IIValue = JSON.toJSONString(jsonObject, SerializerFeature.DisableCircularReferenceDetect);
                jsonArray.add(JSON.parseObject(IIValue));
                jsonArray.add(jsonObject);
                tmp.put("body", jsonArray);
                tmp.put("project", ConfigUtil.canalOrderLocation);
                tmp.put("source", ConfigUtil.canalSource);
                return tmp;
            }
        }).timeWindowAll(Time.seconds(5)).reduce(new ReduceFunction<JSONObject>() {
            @Override
            public JSONObject reduce(JSONObject value1, JSONObject value2) throws Exception {
                JSONArray jsonArray = value1.getJSONArray("body");
                JSONObject res = value2.getJSONArray("body").getJSONObject(0);
                jsonArray.add(res);
                value1.put("body", jsonArray);
                return value1;
            }
        });
        return reduce;
    }
}

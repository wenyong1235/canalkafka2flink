package com.bz365;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;

/**
 * @author : LJF
 * @date : 2020/5/13 10:48
 * @description: 汇总功能流       utils（应用程序）
 * @version: 1.0
 */
                                                            //对数据进行聚合
public class AggregateFunctionStream implements AggregateFunction<String, HashMap<String, Long>,HashMap<String, Long>> {    //implements：实现父类和子类之间继承关系
    HashMap<String, Long> mergeAccumulator = new HashMap<>();

    @Override
    public HashMap<String, Long> createAccumulator() {      //创建累加器
        HashMap<String, Long> map = new HashMap<>();
        return map;
    }

    // 返回新的累加器
    @Override
    public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
        String database = JSON.parseObject(value).getString("database");
        if (!accumulator.containsKey(database)){
            accumulator.put(database,1L);
        }else {
            Long count = accumulator.get(database);
            accumulator.put(database,count + 1L);
        }
        return accumulator;
    }

    @Override
    public HashMap<String, Long> getResult(HashMap<String, Long> accumulator) {
        return accumulator;     //getResult() 是静态方法 , 可以在main函数中直接直接调用(在非本类中可以用 类名.方法名 来调用)而不用声明对象
    }

    // 累加器合并
    @Override
    public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {      //merge()：将新值置于给定键下（如果不存在）或更新具有给定值的现有键（UPSERT）。
        for (String key : b.keySet()){
            if (a.containsKey(key)){
                long count = a.get(key) + b.get(key);
                mergeAccumulator.put(key,count);
            }else {
                mergeAccumulator.put(key,b.get(key));
            }
        }
        return mergeAccumulator;
    }
}

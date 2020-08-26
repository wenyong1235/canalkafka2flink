package com.bz365;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bz365.input.ReadRedis;
import com.bz365.output.WriteRedis;
import com.bz365.output.WriteToWebsocket;
import com.bz365.utils.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author : LJF
 * @date : 2020/5/12 12:42
 * @description: 统计canal采集处理的业务库表的数据量
 * @version: 1.0
 */

public class CanalCount {
    public static transient ValueState<Map<String, String>> sum;
    public volatile static Map<String,String> sumTemp = new ConcurrentHashMap<>();
    public volatile static java.sql.Date day;
    public volatile static HashMap<String,String> map;
    public volatile static Map<String,String> count = new ConcurrentHashMap<>();


    // 处理flink挂掉重新启动count数值为0的问题,启动时从redis读取暂存数据
    static{
        day = TimeUtil.sqlDate();
        map = ReadRedis.readRedis();
        // redis里还没有该数据
        if (map.isEmpty()){
            mapReset();
            redisReset();
        }else {
            // redis里的日期和当前日期不同，将此时redis里的数据写到mysql，并清零redis和map里的值
            String dateRedis = map.get("date");
            SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
            Date date = new Date(System.currentTimeMillis());
            String currentDay = formatter.format(date);
            if (!dateRedis.equals(currentDay)){
                try {
                    writeMysql(map);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                mapReset();
                redisReset();
            }
        }
    }


    public static void canalCount(DataStream<String> source){
        DataStream<String> filter = source.filter(line -> JsonUtil.isJsonObject(line) && (JSON.parseObject(line).getString("database") != null));

        DataStream<Map<String, String>> canalCount = filter.timeWindowAll(Time.seconds(30)).process(new ProcessAllWindowFunction<String, Map<String, String>, TimeWindow>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                Set<Map.Entry<String, String>> entries = map.entrySet();
                Iterator<Map.Entry<String, String>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> next = iterator.next();
                    if (!next.getKey().equals("date")) {
                        count.put(next.getKey(), next.getValue());
                    }
                }
            }

            @Override
            public void process(Context context, Iterable<String> elements, Collector<Map<String, String>> out) throws Exception {
                Iterator<String> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    JSONObject jsonObject = JSON.parseObject(line);
                    String database = "dx-" + jsonObject.getOrDefault("database","IT");
                    if (!count.containsKey(database)) {
                        count.put(database,"1");
                    } else {
                        String tmpCount = count.get(database);
                        count.put(database, String.valueOf(Long.parseLong(tmpCount) + 1L));
                    }
                }
                out.collect(count);
            }
        });


        /*DataStream<HashMap<String, Long>> windowCount = filter.keyBy(line -> JSON.parseObject(line).getString("database")).timeWindow(Time.seconds(30)).aggregate(new AggregateFunctionStream());
        // 累计每一个小窗口的值，保存到state里
        DataStream<Map<String, String>> canalCount = windowCount.timeWindowAll(Time.seconds(2)).process(new ProcessAllWindowFunction<HashMap<String, Long>, Map<String, String>, TimeWindow>() {
            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<Map<String, String>> descriptor = new ValueStateDescriptor<>("canalCount", TypeInformation.of(new TypeHint<Map<String, String>>() {
                }));
                sum = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void process(Context context, Iterable<HashMap<String, Long>> elements, Collector<Map<String, String>> out) throws Exception {
                // 首次启动给sum赋值
                if (sum.value() == null) {
                    sumAssignment();
                    mapReset();
                }

                // 历史值和每个小窗口里的累加值合并
                Iterator<HashMap<String, Long>> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    HashMap<String, Long> next = iterator.next();
                    sumTemp.put("dx-bz_user", String.valueOf(Long.parseLong(sum.value().getOrDefault("dx-bz_user", "0")) + next.getOrDefault("bz_user",0L)));
                    sumTemp.put("dx-bz_order", String.valueOf(Long.parseLong(sum.value().getOrDefault("dx-bz_order", "0")) + next.getOrDefault("bz_order",0L)));
                    sumTemp.put("dx-bz_ware", String.valueOf(Long.parseLong(sum.value().getOrDefault("dx-bz_ware", "0")) + next.getOrDefault("bz_ware",0L)));
                    sumTemp.put("dx-bz_cms", String.valueOf(Long.parseLong(sum.value().getOrDefault("dx-bz_cms", "0")) + next.getOrDefault("bz_cms",0L)));
                    updateSum(sumTemp);
                }
                try {
                    out.collect(sum.value());
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("原因 收集出错");
                }
                out.collect(sumTemp);
            }
        });*/


        // 写redis
        canalCount.addSink(new WriteRedis());

        DataStream<JSONObject> websocket = canalCount.map(new MapFunction<Map<String, String>, JSONObject>() {
            @Override
            public JSONObject map(Map<String, String> value) {
                // 拼接处理发送到web的json数据
                // 数据格式 {"date":"当天日期","dx-bz_user":"user库的数据量","dx-bz_order":"order库的数据量","dx-bz_ware":"ware库的数据量","dx-bz_cms":"cms库的数据量","timestamp":"时间戳","project":"项目名称","source":"数据来源"}
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("date", day.toString());
                jsonObject.put("timestamp", System.currentTimeMillis());
                jsonObject.put("project", ConfigUtil.canalCount);
                jsonObject.put("source", ConfigUtil.canalSource);
                jsonObject.put("dx-bz_user", value.get("dx-bz_user"));
                jsonObject.put("dx-bz_order", value.get("dx-bz_order"));
                jsonObject.put("dx-bz_ware", value.get("dx-bz_ware"));
                jsonObject.put("dx-bz_cms", value.get("dx-bz_cms"));
                return jsonObject;
            }
        });
        // 发送到web后台
        websocket.addSink(new WriteToWebsocket(ConfigUtil.canalCountURL));


        // 一天的时间窗口，定时写mysql，清空redis，清空state，发送0到web后台
        canalCount.windowAll(TumblingProcessingTimeWindows.of(Time.days(1),Time.hours(-8))).process(new ProcessAllWindowFunction<Map<String, String>, HashMap<String, String>, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Map<String, String>> elements, Collector<HashMap<String, String>> out) throws Exception {
                writeMysql();
                // TODO 切换是否使用 AggregateFunction累计每个小窗口的数据
                // sumAssignment();
                resetCount();
                redisReset();
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("date", TimeUtil.sqlDate().toString());
                jsonObject.put("timestamp", System.currentTimeMillis());
                jsonObject.put("project", ConfigUtil.canalCount);
                jsonObject.put("source", ConfigUtil.canalSource);
                jsonObject.put("dx-bz_user", "0");
                jsonObject.put("dx-bz_order", "0");
                jsonObject.put("dx-bz_ware", "0");
                jsonObject.put("dx-bz_cms", "0");
                WriteToWebsocket.clearView(jsonObject.toString());
                setDay();
            }
        });
    }

    public static void redisReset() {
        Jedis jedis = RedisUtil.getJedisInstance();
        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("date", TimeUtil.sqlDate().toString());
        hashMap.put("dx-bz_user","0");
        hashMap.put("dx-bz_order","0");
        hashMap.put("dx-bz_ware","0");
        hashMap.put("dx-bz_cms","0");
        jedis.hmset("canal-dx-canalCount",hashMap);
    }


    private static void mapReset() {
        map.put("date", TimeUtil.sqlDate().toString());
        map.put("dx-bz_user","0");
        map.put("dx-bz_order","0");
        map.put("dx-bz_ware","0");
        map.put("dx-bz_cms","0");
    }


    public static void setDay(){
        day = TimeUtil.sqlDate();
    }


    public static void sumAssignment() {
        Map<String, String> hashMap = new HashMap<>();
        Set<Map.Entry<String, String>> entries = map.entrySet();
        Iterator<Map.Entry<String, String>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            if (! next.getKey().equals("date")){
                hashMap.put(next.getKey(), next.getValue());
            }
        }
        updateSum(hashMap);
    }
    private static synchronized  void updateSum(Map<String, String> value) {
        try {
            sum.update(value);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("sum更新值出错");
        }
    }

    public static void resetCount(){
        count.put("dx-bz_user","0");
        count.put("dx-bz_order","0");
        count.put("dx-bz_ware","0");
        count.put("dx-bz_cms","0");
    }




    public static void writeMysql() throws SQLException, IOException {
        DatabaseConnectionUtil dbConnection = new DatabaseConnectionUtil();
        Connection conn = dbConnection.getConnention();
        // TODO 切换是否使用 AggregateFunction 累计每个窗口的数据
        Set<Map.Entry<String, String>> entries = count.entrySet();
        // Set<Map.Entry<String, String>> entries = sum.value().entrySet();
        Iterator<Map.Entry<String, String>> iterator = entries.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> next = iterator.next();
            if (! next.getKey().equals("date")){
                String sqlW="insert into t_canal_count (division,databasename,count,date) values(?,?,?,?)";
                PreparedStatement pstmW = conn.prepareStatement(sqlW);
                pstmW.setInt(1, 1);
                pstmW.setString(2, next.getKey());
                pstmW.setLong(3,Long.parseLong(next.getValue()));
                pstmW.setDate(4,day);
                pstmW.executeUpdate();
            }
        }conn.close();
        dbConnection.close();
    }


    private static void writeMysql(HashMap<String,String> hashMap) throws SQLException {
        // 判断数据库里是否已经存在这一天 date 的数据，若存在不执行否则写入
        DatabaseConnectionUtil dbConnection = new DatabaseConnectionUtil();
        Connection conn = dbConnection.getConnention();

        Iterator<Map.Entry<String, String>> iterator = hashMap.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, String> next = iterator.next();
            if (! next.getKey().equals("date")){
                String sqlR="select * from t_canal_count where  division=? and databasename=? and date=?";
                PreparedStatement pstmR = conn.prepareStatement(sqlR);
                pstmR.setInt(1, 1);
                pstmR.setString(2,next.getKey());
                pstmR.setDate(3,java.sql.Date.valueOf(hashMap.get("date")));
                ResultSet resultSet = pstmR.executeQuery();
                boolean exist = resultSet.next();

                if (!exist){
                    String sqlW="insert into t_canal_count (division,databasename,count,date) values(?,?,?,?)";
                    PreparedStatement pstmW = conn.prepareStatement(sqlW);
                    pstmW.setInt(1, 1);
                    pstmW.setString(2, next.getKey());
                    pstmW.setLong(3,Long.parseLong(next.getValue()));
                    pstmW.setDate(4,java.sql.Date.valueOf(hashMap.get("date")));
                    pstmW.execute();
                }
            }
        }
        conn.close();
        dbConnection.close();
    }
}

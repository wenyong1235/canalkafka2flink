package com.bz365;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.bz365.input.GetKafakConsumer;
import com.bz365.input.ReadFromMysql;
import com.bz365.output.OutputJsonArray;
import com.bz365.output.WriteToMysql;
import com.bz365.output.WriteToWebsocket;
import com.bz365.utils.ConfigUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author :LJF
 * @date :2020/2/13 14:34
 * @description: 处理mysql通过canal采集到的binlog日志，然后根据需求做一些处理
 * @version: 1.0
 */
                            //通过关联计算数据流与MySQL数据库的内容，输出匹配的结果：地理位置信息如省市等
public class               //通过Kafka消费MySQL，写入JDBC（关系型）,再通过k-v关系写入MySQL，采用json格式写到web，采集数据量并输出
CanalOrigin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();   //环境设置
        // 设置任务失败重启策略 尝试重启3次，每次间隔20s
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.milliseconds(20)));
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        FlinkKafkaConsumer policyUserConsumer = GetKafakConsumer.getKafkaConsumerOrderLocation();   //设置Kafka消费者（固定写法）
        // 从信息的最新处开始处理
        policyUserConsumer.setStartFromLatest();
        //数据流
        DataStream<String> kafkaStream = env.addSource(policyUserConsumer);     //设置数据流：Kafka stream and others
        DataStream<String> DxBzOrderStream = env.addSource(GetKafakConsumer.getKafkaConsumerDxBzOrder().setStartFromLatest());
        DataStream<String> DxBzUserStream = env.addSource(GetKafakConsumer.getKafkaConsumerDxBzUser().setStartFromLatest());
        DataStream<String> DxBzWareStream = env.addSource(GetKafakConsumer.getKafkaConsumerDxBzWare().setStartFromLatest());
        DataStream<String> DxBzCmsStream = env.addSource(GetKafakConsumer.getKafkaConsumerDxBzCms().setStartFromLatest());


        //横线划掉:此类已经过时，不推荐使用，有更好的方法替代它。
        DataStream<Tuple5<String, String, String, String, String>> policyUsers = SourceFilter.sourceFilter(kafkaStream);    //读取Kafka数据流到policy users
        tEnv.registerDataStream("policy_user",policyUsers,"orderID,userID,policyID,tmobile,create_time");   //读取Kafka流的五种到s1：IDs，创建时间   //寄存器流

        JDBCInputFormat t_mobile_location = ReadFromMysql.getMysqlTable();                                      //读取MySQL数据库(手机号，省，市)到JDBC：用于关联计算的母本
        DataStreamSource<Row> mobile_locationDataStream = env.createInput(t_mobile_location);                   //再put到数据流源
        tEnv.registerDataStream("mobile_location",mobile_locationDataStream,"phone,province,city");       //读取表格里的三种到s1：电话，省份，城市


        Table joind = tEnv.sqlQuery(            //将这一堆信息放在表格里
                "select policy_user.orderID," +
                "policy_user.policyID," +
                "mobile_location.province," +
                "mobile_location.city," +
                "policy_user.create_time " +
                "from mobile_location join policy_user " +          //关联
                "on mobile_location.phone=policy_user.tmobile");    //phone=tmobile
        DataStream<Row> rowDataStream = tEnv.toAppendStream(joind, Row.class);  //存储
        rowDataStream.print();      //输出的写法，不用sout
        // 将结果数据写到Mysql
        rowDataStream.addSink(new WriteToMysql());      //进行关联计算

        // 发送数据到web后台
        SingleOutputStreamOperator<JSONObject> orderLocationJson = rowDataStream.map(new MapFunction<Row, JSONObject>() {       //采用map的k-v关系
            JSONObject jsonObject = new JSONObject();           //创建json对象
            @Override
            public JSONObject map(Row value) throws Exception {     //加入value   //紫色：和配置文件有关
                jsonObject.put("orderID", value.getField(0).toString());
                jsonObject.put("policyID", value.getField(1).toString());
                jsonObject.put("province", value.getField(2).toString());
                jsonObject.put("city", value.getField(3).toString());
                jsonObject.put("create_time", value.getField(4).toString());

                jsonObject.put("project", ConfigUtil.canalOrderLocation);
                jsonObject.put("source", ConfigUtil.canalSource);
                return jsonObject;
            }
        });
        orderLocationJson.addSink(new WriteToWebsocket(ConfigUtil.orderLocationURL));


        // 统计canal采集处理的业务库表的数据量
//        DataStream<String> union = DxBzOrderStream;  //.union(DxBzUserStream).union(DxBzWareStream).union(DxBzCmsStream);
//        CanalCount.canalCount(union);

        env.execute("实时订单地理信息获取;统计binlog数据量");
    }
}

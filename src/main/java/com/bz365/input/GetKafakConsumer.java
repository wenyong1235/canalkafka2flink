package com.bz365.input;

import com.bz365.utils.ConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author :LJF
 * @date :2020/2/13 15:05
 * @description:
 * @version: 1.0
 */
public class GetKafakConsumer {     //Flink读取Kafka生产-消费(固定)：设变量。设参数。返回值。
    public static FlinkKafkaConsumer getKafkaConsumerOrderLocation(){
        Properties propsOrderLocation = new Properties();
        propsOrderLocation.setProperty("bootstrap.servers", ConfigUtil.kafkaBootstrapInDxOrderLocation);
        propsOrderLocation.setProperty("group.id", ConfigUtil.kafkaGroupInDxOrderLocation);
        FlinkKafkaConsumer<String> policyUserConsumer = new FlinkKafkaConsumer<>(ConfigUtil.kafkaTopicInDxOrderLocation, new SimpleStringSchema(), propsOrderLocation);
        return policyUserConsumer;
    }

    // 获取大象四个库对应的kafka topic信息
    public static FlinkKafkaConsumer getKafkaConsumerDxBzUser(){
        Properties propsOrderLocation = new Properties();
        propsOrderLocation.setProperty("bootstrap.servers", ConfigUtil.kafkaBootstrapInDxCanalCount);
        propsOrderLocation.setProperty("group.id", ConfigUtil.kafkaGroupInDxCanalCount);
        FlinkKafkaConsumer<String> UserConsumer = new FlinkKafkaConsumer<>(ConfigUtil.kafkaTopicInDxCanalCountBz_User, new SimpleStringSchema(), propsOrderLocation);
        return UserConsumer;
    }


    public static FlinkKafkaConsumer getKafkaConsumerDxBzOrder(){
        Properties propsOrderLocation = new Properties();
        propsOrderLocation.setProperty("bootstrap.servers", ConfigUtil.kafkaBootstrapInDxCanalCount);
        propsOrderLocation.setProperty("group.id", ConfigUtil.kafkaGroupInDxCanalCount);
        FlinkKafkaConsumer<String> OrderConsumer = new FlinkKafkaConsumer<>(ConfigUtil.kafkaTopicInDxCanalCountBz_Order, new SimpleStringSchema(), propsOrderLocation);
        return OrderConsumer;
    }


    public static FlinkKafkaConsumer getKafkaConsumerDxBzWare(){
        Properties propsOrderLocation = new Properties();
        propsOrderLocation.setProperty("bootstrap.servers", ConfigUtil.kafkaBootstrapInDxCanalCount);
        propsOrderLocation.setProperty("group.id", ConfigUtil.kafkaGroupInDxCanalCount);
        FlinkKafkaConsumer<String> WareConsumer = new FlinkKafkaConsumer<>(ConfigUtil.kafkaTopicInDxCanalCountBz_Ware, new SimpleStringSchema(), propsOrderLocation);
        return WareConsumer;
    }


    public static FlinkKafkaConsumer getKafkaConsumerDxBzCms(){
        Properties propsOrderLocation = new Properties();
        propsOrderLocation.setProperty("bootstrap.servers", ConfigUtil.kafkaBootstrapInDxCanalCount);
        propsOrderLocation.setProperty("group.id", ConfigUtil.kafkaGroupInDxCanalCount);
        FlinkKafkaConsumer<String> CmsConsumer = new FlinkKafkaConsumer<>(ConfigUtil.kafkaTopicInDxCanalCountBz_Cms, new SimpleStringSchema(), propsOrderLocation);
        return CmsConsumer;
    }





}

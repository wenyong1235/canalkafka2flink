package com.bz365.utils;

import java.util.ResourceBundle;

public class ConfigUtil {
    private static ResourceBundle config = ResourceBundle.getBundle("conf");    //连接配置文件conf

    // 获取mysql的配置信息
    public static String mysqlDriverIn = config.getString("mysql.Driver.In");
    public static String mysqlUserIn = config.getString("mysql.user.In");
    public static String mysqlUrlIn = config.getString("mysql.url.In");
    public static String mysqlPSWIn= config.getString("mysql.password.In");

    public static String mysqlDriverOut = config.getString("mysql.Driver.Out");
    public static String mysqlUserOut = config.getString("mysql.user.Out");
    public static String mysqlUrlOut = config.getString("mysql.url.Out");
    public static String mysqlPSWOut= config.getString("mysql.password.Out");


    // 获取kafka的配置信息
    public static String kafkaBootstrapInDxOrderLocation = config.getString("kafka.bootstrap.in.dx.order_location");
    public static String kafkaGroupInDxOrderLocation = config.getString("kafka.group.in.dx.order_location");
    public static String kafkaTopicInDxOrderLocation = config.getString("kafka.topic.in.dx.order_location");

    public static String kafkaBootstrapInDxCanalCount = config.getString("kafka.bootstrap.in.dx.canal-count");
    public static String kafkaGroupInDxCanalCount = config.getString("kafka.group.in.dx.canal-count");
    public static String kafkaTopicInDxCanalCountBz_User = config.getString("kafka.topic.in.dx.canal-count.bz_user");
    public static String kafkaTopicInDxCanalCountBz_Order = config.getString("kafka.topic.in.dx.canal-count.bz_order");
    public static String kafkaTopicInDxCanalCountBz_Ware = config.getString("kafka.topic.in.dx.canal-count.bz_ware");
    public static String kafkaTopicInDxCanalCountBz_Cms = config.getString("kafka.topic.in.dx.canal-count.bz_cms");


    // 获取redis配置信息
    public static String redisHost = config.getString("redis.host");
    public static String redisPort = config.getString("redis.port");
    public static String redisDXCanalCount = config.getString("redis.canal.dx.count.key");


    // 获取websocketurl地址
    public static String canalCountURL = config.getString("webSocketURL.canalCount");
    public static String orderLocationURL = config.getString("webSocketURL.orderLocation");


    // 获取json数据里区分数据来源的标志信息
    public static String canalOrderLocation = config.getString("WS.canal.JSONData.project.canal-order-location");
    public static String canalCount = config.getString("WS.canal.JSONData.project.canal-count");
    public static String canalSource = config.getString("WS.canal.JSONData.source.dx");

}

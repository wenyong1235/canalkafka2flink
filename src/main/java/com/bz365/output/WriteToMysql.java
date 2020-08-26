package com.bz365.output;

import com.bz365.utils.ConfigUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author :LJF
 * @date :2020/2/13 14:35
 * @description: 用来将flink里的流表(订单信息用户表)和t_mobile_location关联计算出的
 *               订单地理位置信息数据写到mysql的t_order_location表里
 * @version: 1.0
 */

public class WriteToMysql extends RichSinkFunction<Row> {
    private Connection connect = null;
    private PreparedStatement ps = null;

    // 连接数据库
    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        Class.forName(ConfigUtil.mysqlDriverOut);
        connect = DriverManager.getConnection(ConfigUtil.mysqlUrlOut, ConfigUtil.mysqlUserOut, ConfigUtil.mysqlPSWOut); //与配置文件连接
        String sql = "insert into t_order_location(order_id," +
                "policy_id," +
                "province," +
                "city," +
                "create_time) value(?,?,?,?,?);";    //写入表t_order_location：5种
        ps = connect.prepareStatement(sql);
    }

    // 执行数据写入的操作
    @Override
    public void invoke(Row value, Context context) throws SQLException {    //throws：当前方法可能会抛出异常，但是不知道如何处理该异常，
                                                                            // 就将该异常交由调用这个方法的的上一级使用者处理
        //将row里的每一个字段都和mysql表里的字段一一对应上
        ps.setString(1,value.getField(0).toString());
        ps.setString(2,value.getField(1).toString());
        ps.setString(3,value.getField(2).toString());
        ps.setString(4,value.getField(3).toString());
        ps.setString(5,value.getField(4).toString());
        //ps.setString(6,value.getField(5).toString());
        ps.executeUpdate();
    }

    // 关闭连接
    @Override
    public void close() throws Exception{
        try {
            super.close();
            if (connect != null){
                connect.close();
            }
            if (ps != null){
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();    //在命令行打印，异常信息在程序中出错的位置及原因。
        }
    }
}


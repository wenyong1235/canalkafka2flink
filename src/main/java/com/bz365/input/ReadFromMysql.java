package com.bz365.input;

import com.bz365.utils.ConfigUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

/**
 * @author :LJF
 * @date :2020/2/13 15:17
 * @description:
 * @version: 1.0
 */
public class ReadFromMysql {
    public static JDBCInputFormat getMysqlTable(){
        TypeInformation[] fieldType = new TypeInformation[]{    //flink类型系统
                /*
                `phone`  COMMENT '手机号前7位',
                `province` ' COMMENT '省',
                `city` ' COMMENT '市',*/
                BasicTypeInfo.STRING_TYPE_INFO,     //基础类型信息
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldType);       //转化为行类型信息

        JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()      //JDBC输入格式
                .setDrivername(ConfigUtil.mysqlDriverIn)        //public class ConfigUtil
                .setDBUrl(ConfigUtil.mysqlUrlIn)                //CanalCount 和 CanalOrigin
                .setUsername(ConfigUtil.mysqlUserIn)
                .setPassword(ConfigUtil.mysqlPSWIn)
                .setQuery("select phone,province,city from t_mobile_location")
                .setRowTypeInfo(rowTypeInfo)        //将flink类型，插入JDBC中
                .finish();
        return jdbcInput;
    }

}

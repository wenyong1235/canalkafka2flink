package com.bz365.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author :LJF
 * @date :2020/2/13 16:11
 * @description: 用来处理时间相关的工具类
 * @version: 1.0
 */
public class TimeUtil {
    /**
     * 用来获取当前时间
     */
    public static String getDateTime(){
        Date date = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateTime = df.format(date);
        return dateTime;
    }

    public static String date(){
        // 格式2020-05-06
        java.util.Date utilDate = new Date();
        java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
        return String.valueOf(sqlDate);
    }


    public static java.sql.Date sqlDate(){
        java.util.Date utilDate = new Date();
        java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
        return sqlDate;
    }
}

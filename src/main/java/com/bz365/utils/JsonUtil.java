package com.bz365.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

/**
 * @author :LJF
 * @date :2020/2/13 16:12
 * @description: 处理json数据工具类
 * @version: 1.0
 */
public class JsonUtil {
    // 判断字符串是否可以转成json对象
    public static boolean isJsonObject(String str){
        if (StringUtils.isBlank(str)){
            return false;
        }else {
            try {
                JSONObject jsonObject = JSON.parseObject(str);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }

    // 判断字符串是否是否可以转成json数组
    public static boolean isJsonArray(String str){
        if (StringUtils.isBlank(str)){
            return false;
        }else {
            try {
                JSONArray jsonArray = JSONArray.parseArray(str);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }


     /**
      * 判断json里是否包含特定的key或者value,不过这个只能处理一层json如果有多层
      * 或者json数组是不行的
      */
    public static boolean isJsonContainKey(JSONObject jsonObject, String key){
        boolean status = false;
        if (jsonObject.containsKey(key)){
            status = true;
        }
        return status;
    }

    public static boolean isJsonContainValue(JSONObject jsonObject, String value){
        boolean status = false;
        if (jsonObject.containsValue(value)){
            status = true;
        }
        return status;
    }
}

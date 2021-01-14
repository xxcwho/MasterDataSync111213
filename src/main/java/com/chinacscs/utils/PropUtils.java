package com.chinacscs.utils;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author wangjj
 * @date 2016/9/22 15:56
 * @description properties文件处理工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class PropUtils {

    final static String PROPERTY_FILE_NAME = "Application.properties";

    /**
     * classpath中的Application.properties对应的Properties对象
     */
    private static Properties props = new Properties();

    static {
        try {
            ClassLoader cl = PropUtils.class.getClassLoader();
            InputStream in = cl.getResourceAsStream(PROPERTY_FILE_NAME);
            props.load(in);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 读取一个property，返回String
     *
     * @param key property的key
     * @return property返回的String类型值
     */
    public static String getString(String key) {
        return props.getProperty(key);
    }

    /**
     * 读取一个property，返回Integer
     *
     * @param key property的key
     * @return property返回的Integer类型值
     */
    public static Integer getInteger(String key) {
        String value = props.getProperty(key);
        if (StringUtils.isEmpty(value)) {
            return null;
        } else {
            return Integer.valueOf(value);
        }
    }

    public static Boolean getBoolean(String key) {
        String value = props.getProperty(key);
        //如果没有配置对应参数，默认返回false
        if (StringUtils.isEmpty(value)) {
            return false;
        } else {
            return Boolean.valueOf(value);
        }
    }

    public static Integer getOrDefault(String key, Integer defaultValue) {
        String value = props.getProperty(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }
}

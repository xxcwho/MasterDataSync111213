package com.chinacscs.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author wangjj
 * @date 2016/9/22 15:57
 * @description 字符串处理工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class StringUtils {
    /**
     * 将classpath中的文件内容读取成一个字符串
     * @param fileName 文件在classpath中的路径
     * @return 文件内容
     */
    public static String getStringFromResource(String fileName) {
        try {
            ClassLoader cl = StringUtils.class.getClassLoader();
            InputStream in = cl.getResourceAsStream(fileName);
            BufferedReader buf = new BufferedReader(new InputStreamReader(in));
            StringBuilder str = new StringBuilder();
            String line;
            while ((line = buf.readLine()) != null) {
                str.append(line).append("\n");
            }
            buf.close();
            return str.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 判断字符串是否为空
     * @param str 字符串
     * @return 是否为空
     */
    public static boolean isEmpty(String str) {
        if (str == null || "".equals(str) || "null".equals(str.toLowerCase())) {
            return true;
        } else {
            return false;
        }
    }

    public static String lpad(String str, int destLength, char c) {
        int srcLength = str.length();
        if(srcLength >= destLength) {
            return str;
        } else {
            for(int i = 1; i <= destLength - srcLength; i++) {
                str = c + str;
            }
            return str;
        }
    }

    public static String rpad(String str, int destLength, char c) {
        int srcLength = str.length();
        if(srcLength >= destLength) {
            return str;
        } else {
            for(int i = 1; i <= destLength - srcLength; i++) {
                str = str + c;
            }
            return str;
        }
    }
}

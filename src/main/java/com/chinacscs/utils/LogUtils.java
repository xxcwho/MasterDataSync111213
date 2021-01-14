package com.chinacscs.utils;

import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author wangjj
 * @date 2016/12/27 15:54
 * @description Log工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class LogUtils {
    public static void logException(Logger logger, Throwable e) {
        e.printStackTrace();
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw, true));
        String str = sw.toString();
        logger.error(str);
    }
}

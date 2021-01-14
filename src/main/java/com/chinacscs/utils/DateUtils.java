package com.chinacscs.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author wangjj
 * @date 2016/12/20 15:54
 * @description 日期工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class DateUtils {

    private static final String FORMAT_DATE = "yyyyMMdd";
    private static final String FORMAT_DATETIME = "yyyyMMddHHmmss";
    private static final String FORMAT_DATETIME_PRETTY = "yyyy-MM-dd HH:mm:ss";

    public static String getCurrentDate() {
        SimpleDateFormat yyyyMMdd = new SimpleDateFormat(FORMAT_DATE);
        return yyyyMMdd.format(new Date());
    }

    public static String getCurrentTime() {
        SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat(FORMAT_DATETIME);
        return yyyyMMddHHmmss.format(new Date());
    }

    public static String getCurrentTimePretty() {
        SimpleDateFormat yyyyMMddHHmmssPretty = new SimpleDateFormat(FORMAT_DATETIME_PRETTY);
        return yyyyMMddHHmmssPretty.format(new Date());
    }

    public static Date toDate(String dateStr, String format) throws ParseException {
        return new SimpleDateFormat(format).parse(dateStr);
    }

    public static String toString(Date date, String format) throws ParseException {
        return new SimpleDateFormat(format).format(date);
    }
}
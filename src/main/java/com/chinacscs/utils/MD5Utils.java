package com.chinacscs.utils;

/**
 * @author wangjj
 * @date 2016/9/22 15:56
 * @description MD5文件处理工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class MD5Utils {

    /** 用来将字节转换成 16 进制表示的字符 */
    private static final char[] hexDigits = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};


    /**
     * @param md5b
     * @return
     */
    public static String getHexStr(byte[] md5b) {

        String s = "";
        // 每个字节用 16 进制表示的话，使用两个字符，所以表示成 16 进制需要 32 个字符
        char[] str = new char[16 * 2];
        // 表示转换结果中对应的字符位置
        int k = 0;
        // 从第一个字节开始，对 MD5 的每一个字节, 转换成 16 进制字符的转换
        for (int i = 0; i < 16; i++) {
            // 取第 i 个字节
            byte byte0 = md5b[i];
            // 取字节中高 4 位的数字转换, >>> 为逻辑右移，将符号位一起右移
            str[k++] = hexDigits[byte0 >>> 4 & 0xf];
            // 取字节中低 4 位的数字转换
            str[k++] = hexDigits[byte0 & 0xf];
        }
        // 换后的结果转换为字符串
        s = new String(str);

        return s;
    }

    /**
     * @param source
     * @return
     */
    public static String getMD5Str(byte[] source) {
        String s = null;

        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
            md.update(source);
            // MD5 的计算结果是一个 128 位的二进制长整数，用字节表示就是 16 个字节
            byte[] tmp = md.digest();
            s = getHexStr(tmp);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return s;
    }


}

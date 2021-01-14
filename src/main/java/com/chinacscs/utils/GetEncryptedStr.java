package com.chinacscs.utils;

/**
 * @author wangjj
 * @date 2017/6/14 17:26
 * @description 获取加密字符串
 * @copyright(c) chinacscs all rights reserved
 */
public class GetEncryptedStr {
    public static void main(String[] args) {
        if(args.length != 1) {
            throw new RuntimeException("参数必须为一个！");
        }
        String str = args[0];
        System.out.println(AESUtils.encrypt(str));
    }
}
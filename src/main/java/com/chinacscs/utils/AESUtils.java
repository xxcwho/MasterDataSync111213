package com.chinacscs.utils;

import org.apache.commons.net.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * @author wangjj
 * @date 2017/6/14 17:39
 * @description TODO
 * @copyright(c) chinacscs all rights reserved
 */
public class AESUtils {

    private static final String KEY="db@chinacscs.com";

    /**
     * 加密
     *
     * @param sSrc
     * @return
     */
    public static String encrypt(String sSrc){
        try {
            byte[] raw = KEY.getBytes();
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            //"算法/模式/补码方式"
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            //使用CBC模式，需要一个向量iv，可增加加密算法的强度
            IvParameterSpec iv = new IvParameterSpec("0102030405060708".getBytes());
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
            byte[] encrypted = cipher.doFinal(sSrc.getBytes());
            //此处使用BAES64做转码功能，同时能起到2次加密的作用。
            return Base64.encodeBase64String(encrypted);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 解密
     *
     * @param sSrc
     * @return
     */
    public static String decrypt(String sSrc){
        try {
            if(sSrc == null) {
                return null;
            }
            byte[] raw = KEY.getBytes("ASCII");
            SecretKeySpec skeySpec = new SecretKeySpec(raw, "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            IvParameterSpec iv = new IvParameterSpec("0102030405060708".getBytes());
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            //先用bAES64解密
            byte[] encrypted1 = Base64.decodeBase64(sSrc);
            byte[] original = cipher.doFinal(encrypted1);
            String originalString = new String(original);
            return originalString;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
}

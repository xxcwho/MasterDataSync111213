package com.chinacscs.utils;

import org.apache.commons.codec.binary.Hex;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author wangjj
 * @date 2017/5/12 17:32
 * @description 文件工具类
 * @copyright(c) chinacscs all rights reserved
 */
public class FileUtils {

    static MessageDigest md = null;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ne) {
        }
    }

    public static String getMd5ByFile(File file) throws IOException, NoSuchAlgorithmException {
        String value = null;
        try (FileInputStream in = new FileInputStream(file)) {
            MappedByteBuffer byteBuffer = in.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(byteBuffer);
            BigInteger bi = new BigInteger(1, md5.digest());
            value = bi.toString(16);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return value;
    }

    public static String md5(File f) {
        try (FileInputStream fis = new FileInputStream(f)) {
            //100KB each time
            byte[] buffer = new byte[102400];
            int length;
            long loopCount = 0;
            while ((length = fis.read(buffer)) != -1) {
                md.update(buffer, 0, length);
                loopCount++;
            }
            return new String(Hex.encodeHex(md.digest()));
        } catch (FileNotFoundException e) {
            return null;
        } catch (IOException e) {
            return null;
        }
    }
}

package com.zz.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @Author pdp
 * @Description MD5工具类
 **/

public class MD5Util {

    /**
     * @Author pdp
     * @Description 全局数组
     * @Param
     * @return
     **/

    private final static String[] strDigits = { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d",
            "e", "f" };

    public MD5Util() {
    }

    /**
     * @Author pdp
     * @Description 返回形式为数字和字符串
     * @Param [bByte]
     * @return java.lang.String
     **/

    private static String byteToArrayString(byte bByte) {
        int iRet = bByte;
        if (iRet < 0) {
            iRet += 256;
        }
        int iD1 = iRet / 16;
        int iD2 = iRet % 16;
        return strDigits[iD1] + strDigits[iD2];
    }

    /**
     * @Author pdp
     * @Description 转换字节数组为16进制字串
     * @Param [bByte]
     * @return java.lang.String
     **/

    private static String byteToString(byte[] bByte) {
        StringBuffer sBuffer = new StringBuffer();
        for (int i = 0; i < bByte.length; i++) {
            sBuffer.append(byteToArrayString(bByte[i]));
        }
        return sBuffer.toString();
    }

    /**
     * @Author pdp
     * @Description 将给定的字符串经过md5加密后返回
     * @Param [str]
     * @return java.lang.String
     **/

    public static String getMD5Code(String str) {
        String resultString = null;
        try {
            // 将给定字符串追加一个静态字符串，以提高复杂度
            resultString = new String(str);
            MessageDigest md = MessageDigest.getInstance("MD5");
            // md.digest() 该函数返回值为存放哈希值结果的byte数组
            resultString = byteToString(md.digest(resultString.getBytes()));
        } catch (NoSuchAlgorithmException ex) {
            ex.printStackTrace();
        }
        return resultString;
    }
}

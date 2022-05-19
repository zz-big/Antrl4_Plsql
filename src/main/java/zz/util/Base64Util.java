package zz.util;

import org.apache.commons.codec.binary.Base64;

/**
 * Description:
 *
 * @author zz
 * @date 2021/10/19
 */
public class Base64Util {
    // 加密
    public static String getBase64(String str) {
        String encodeBase64String = Base64.encodeBase64String(str.getBytes());
        return encodeBase64String;
    }

    // 解密
    public static String getFromBase64(String s) {
        byte[] decodeBase64 = Base64.decodeBase64(s);
        s = new String(decodeBase64);
        return s;
    }
}

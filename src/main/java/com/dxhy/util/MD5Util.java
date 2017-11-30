package com.dxhy.util;

/**
 * Created by thinkpad on 2017/3/15.
 */

import java.security.MessageDigest;

public class MD5Util {
    private static final String[] hexDigits = {"0", "1", "2", "3", "4", "5",
            "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

    public static String byteArrayToHexString(byte[] b) {
        StringBuffer resultSb = new StringBuffer();
        for (int i = 0; i < b.length; i++) {
            resultSb.append(byteToHexString(b[i]));
        }
        return resultSb.toString();
    }

    private static String byteToHexString(byte b) {
        int n = b;
        if (n < 0)
            n += 256;
        int d1 = n / 16;
        int d2 = n % 16;
        return hexDigits[d1] + hexDigits[d2];
    }

    public static String MD5Encode(String origin) {
        String resultString = null;
        try {
            resultString = new String(origin);
            MessageDigest md = MessageDigest.getInstance("MD5");
            String hashCodeString = String.valueOf(stringHashCode(resultString));
            int randomNum = Integer.parseInt(hashCodeString.length() > 5 ? hashCodeString.substring(0, 5) : hashCodeString);
            resultString = String.valueOf(randomNum > 99999 ? 99999 : randomNum < 10000 ? 10000 : randomNum) + "_" + byteArrayToHexString(md.digest(resultString.getBytes()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return resultString;
    }

    public static long stringHashCode(String secureString) {
        int hashcodeInt = secureString.hashCode();
        long hashcode = RandomUtils.FNVhash64(hashcodeInt);
        return hashcode;
    }

    public static void main(String[] args) {
        String aa = "04062214580900010001";
        int j = 0;
        for (int i = 0; i < 10000; i++) {
            String result = MD5Encode(aa + i);
            System.out.println(result);
            if (result.startsWith("9")) {
                j++;
            }
        }
        System.out.println("j=" + j);
    }
}

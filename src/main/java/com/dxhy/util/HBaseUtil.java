package com.dxhy.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Random;
import java.util.UUID;

import com.alibaba.fastjson.JSONObject;
import com.dxhy.entity.Invoice;


public class HBaseUtil {
	
    public static Random random = new Random();

    public static MessageDigest md = null;

    public static String getRowKey() {

        int rand = random.nextInt(89999) + 10000;

        return rand + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    public static String str2Md5(String timestamp) {
        byte[] bytes = String.valueOf(System.currentTimeMillis()).getBytes();
        md.update(bytes);
        byte[] buf = md.digest();
        String md32 = new BigInteger(1, buf).toString(16);
        String md16 = md32.substring(8, 24);
        return md16;
    }


    public static Invoice setProperties(JSONObject json, Class<Invoice> invoiceClass) {

        Invoice invoice;
        try {
            invoice = invoiceClass.newInstance();
            Field[] fields = invoiceClass.getDeclaredFields();
            String type = null;
            for (Field field : fields) {

                String name = field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
                type = field.getGenericType().toString();
                try {
                    if (type.equals("class java.lang.String")) {
                        if (json.containsKey(name) && !json.get(name).toString().equals("")) {
                            Method method = invoiceClass.getMethod("set" + name, String.class);
                            method.invoke(invoice, json.getString(name));
                        } else {
                            continue;
                        }
                    } else if (type.equals("int")) {
                        if (json.containsKey(name) && !json.get(name).toString().equals("")) {
                            Method method = invoiceClass.getMethod("set" + name, int.class);
                            method.invoke(invoice, json.getInteger(name));
                        } else {
                            continue;
                        }
                    } else if (type.equals("long")) {
                        if (json.containsKey(name) && !json.get(name).toString().equals("")) {
                            Method method = invoiceClass.getMethod("set" + name, long.class);
                            method.invoke(invoice, json.getLongValue(name));
                        } else {
                            continue;
                        }
                    }
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                } catch (SecurityException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
            return invoice;
        } catch (InstantiationException e1) {
            e1.printStackTrace();
        } catch (IllegalAccessException e1) {
            e1.printStackTrace();
        }
        return null;
    }

}

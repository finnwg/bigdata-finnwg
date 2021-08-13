package com.sundear.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

public class ApplicationUtils {

    /*//定义配置项
    private static Properties pro = new Properties();
    //定义类加载器加载配置文件
    private static InputStream kafkaPor = ClassLoader.getSystemClassLoader().getResourceAsStream("application.properties");

    //返回配置项的方法
    public static String getValue(String key) {
        if(null == pro) {
            return "配置项创建失败！！！";
        }
        if(null == kafkaPor) {
            return "类加载器加载配置文件失败！！！";
        }
        try {
            pro.load(kafkaPor);
        } catch (IOException e) {
            System.err.println("配置项加载配置文件失败！！！");
            e.printStackTrace();
        }

        return pro.getProperty(key);
    }*/


    //读取配置文件
    private static ResourceBundle bundle = ResourceBundle.getBundle("application");

    public static String getValue(String key) {
        if(null == bundle) {
            return "配置文件加载失败！！！";
        }
        return bundle.getString(key);
    }

    /*public static void main(String[] args) {
        System.out.println(getValue("kafka.groupId"));
    }*/
}

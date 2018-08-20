package com. atguigu.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
//将项目所需的配置文件外部化，解耦，方便配置
public class PropertiesUtil {
    public static Properties properties = null;
    static{
        try {
            // 加载配置属性
            InputStream inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties");
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return properties.getProperty(key);
    }
}

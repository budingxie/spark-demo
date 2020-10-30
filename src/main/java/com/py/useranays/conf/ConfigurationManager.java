package com.py.useranays.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author pengyou@xiaomi.com
 * @date 2020/8/25
 */
public class ConfigurationManager {

    private static Properties prop = new Properties();

    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("conf.properties");
            prop.load(in);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 获取指定key对应的value
     *
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 获取整数类型的配置项
     *
     * @param key
     * @return value
     */
    public static Integer getInteger(String key) {
        try {
            String value = prop.getProperty(key);
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        try {
            String value = prop.getProperty(key);
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return value
     */
    public static Long getLong(String key) {
        try {
            String value = prop.getProperty(key);
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
            return 0L;
        }
    }

}

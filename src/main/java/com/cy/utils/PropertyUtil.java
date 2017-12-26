package com.cy.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * 读取配置文件工具类
 * Created by cy on 2017/12/25 23:14.
 */
public class PropertyUtil {
    private static Logger log = LoggerFactory.getLogger(PropertyUtil.class);
    private static final Properties prop = new Properties();
    private static final PropertyUtil propertyUtil = new PropertyUtil();
    private PropertyUtil(){}
   /* static {
        try {
            prop.load(PropertyUtil.class.getClassLoader().getResourceAsStream("conf.properties"));
        } catch (IOException e) {
            log.error("获取流失败：" + e);
            e.printStackTrace();
        }
    }*/
    public static PropertyUtil getPropertyUtil(String properties){
        try {
            prop.load(PropertyUtil.class.getClassLoader().getResourceAsStream(properties));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return propertyUtil;
    }
    public String getValue(String key){
        if (StringUtils.isBlank(key)){
            log.error("key无效！");
            return null;
        }
        return prop.getProperty(key);
    }
}

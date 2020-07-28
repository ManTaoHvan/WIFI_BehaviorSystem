package com.cn.shool.bigdata.bigdata.common.config;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class ConfigUtil {

    //单例
    private static final Logger LOG = Logger.getLogger(ConfigUtil.class);

    //定义一个私有的静态变量
    private static volatile ConfigUtil configUtil;

    //获取示例对象
    public static ConfigUtil getInstance(){
        //使用双重否定
        if(configUtil == null){
             synchronized (ConfigUtil.class){
                if(configUtil == null){
                    configUtil = new ConfigUtil();
                }
            }
        }
        return configUtil;
    }

    //读取配置文件
    public Properties getProperties(String path){
        Properties properties  = new Properties();
        try{
            LOG.info("开始加载配置文件" + path);
            InputStream insss = this.getClass().getClassLoader().getResourceAsStream(path);
            properties.load(insss);
            LOG.info("加载配置文件" + path +"成功");
        }catch (IOException e){
            LOG.error("开始加载配置文件" + path + "失败");
            LOG.error(null,e);
        }
        return properties;
    }


    public static void main(String[] args) {
        String path = "test/kafka/kafka-server-config.properties";
        Properties properties = ConfigUtil.getInstance().getProperties(path);
        String value = properties.get("metadata.broker.list").toString();
        System.out.println(value);
    }

}

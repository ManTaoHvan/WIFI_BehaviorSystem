package com.cn.shool.bigdata.bigdata.kafka.config;

import com.cn.shool.bigdata.bigdata.common.config.ConfigUtil;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * @author:
 * @description: 构造ProducerConfig
 * @Date:Created in 2019-10-
 */
public class KafkaConfig {

    //单例
    private static final Logger LOG = Logger.getLogger(KafkaConfig.class);

    private static final String DEFUALT_CONFIG_PATH = "test/kafka/kafka-server-config.properties";
    //定义一个私有的静态变量
    private static volatile KafkaConfig kafkaConfig = null;
    private ProducerConfig producerConfig;
    private Properties properties;


    //私有构造方法
    private KafkaConfig(){
         //对producerConfig进行初始化
        LOG.info("开始实例化producerConfig");
        properties = ConfigUtil.getInstance().getProperties(DEFUALT_CONFIG_PATH);
        producerConfig = new ProducerConfig(properties);
         LOG.info("实例化producerConfig完成");
    }

    //对外提供一个公共接口
    public static KafkaConfig getInstance(){
        //使用双重否定
        if(kafkaConfig == null){
            synchronized (KafkaConfig.class){
                if(kafkaConfig == null){
                    kafkaConfig = new KafkaConfig();
                }
            }
        }
        return kafkaConfig;
    }

    //返回producerConfig
    public ProducerConfig getProducerConfig(){
        return producerConfig;
    }


    public static void main(String[] args) {
        ProducerConfig producerConfig = KafkaConfig.getInstance().getProducerConfig();
        ProducerConfig producerConfig1 = KafkaConfig.getInstance().getProducerConfig();
        
    }



}

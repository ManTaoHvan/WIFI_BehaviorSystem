package com.cn.shool.bigdata.bigdata.kafka.producer;

import com.cn.shool.bigdata.bigdata.common.config.ConfigUtil;
import com.cn.shool.bigdata.bigdata.kafka.config.KafkaConfig;
import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class StringProducer {
    private static final Logger LOG = Logger.getLogger(ConfigUtil.class);
    /**
     *  字符串发送
     * @param topic   kafkatopic
     * @param line    发送的json数据
     */
    public static void producer(String topic,String line) throws Exception{
        //构造kafka消息
        try {
            KeyedMessage<String,String> KeyedMessage = new KeyedMessage<>(topic,line);
            ProducerConfig producerConfig = KafkaConfig.getInstance().getProducerConfig();
            Producer<String, String> producer = new Producer<>(producerConfig);
            producer.send(KeyedMessage);
            LOG.info("向kafka "+topic +"发送数据成功");
        } catch (Exception e) {
            LOG.error("向kafka "+topic +"发送数据失败");
            LOG.error(null,e);
        }
    }






    public static void main(String[] args) throws Exception {
        StringProducer.producer("test1","11111111111");
    }


}

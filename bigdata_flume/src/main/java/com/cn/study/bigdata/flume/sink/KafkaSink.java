package com.cn.shool.bigdata.bigdata.flume.sink;

import com.cn.shool.bigdata.bigdata.kafka.producer.StringProducer;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class KafkaSink extends AbstractSink implements Configurable {
    private static final Logger LOG = Logger.getLogger(MySink.class);

    private String kafkaTopic;

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        LOG.error("====KafkaSink启动=======");
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            Event event = ch.take();
            if(event == null){
                txn.rollback();
                return status.BACKOFF;
            }
            String line = new String(event.getBody());
            //TODO 将数据推送到kafka
            StringProducer.producer(kafkaTopic,line);
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        kafkaTopic = context.getString("kafkaTopic");
        LOG.error("kafkaTopic============" + kafkaTopic);
    }
}

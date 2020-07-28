package com.cn.shool.bigdata.bigdata.flume.sink;

import com.cn.shool.bigdata.bigdata.flume.source.FolderSource;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class MySink extends AbstractSink implements Configurable {
    private static final Logger LOG = Logger.getLogger(MySink.class);

    @Override
    public void configure(Context context) {
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        // Start transaction
        LOG.error("==========SINK-88============");
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        LOG.error("==========SINK-99============");
        txn.begin();
        try {
            LOG.error("==========SINK-0============");
            // This try clause includes whatever Channel operations you want to do
            Event event = ch.take();
            LOG.error("==========SINK-1============");
            if(event == null){
                LOG.error("==========SINK-2============");
                txn.rollback();
                return status.BACKOFF;
            }
            LOG.error("==========SINK-3============");
            String line = new String(event.getBody());
            LOG.error("sink获取到的数据=======" + line);
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            // Log exception, handle individual exceptions as needed
            status = Status.BACKOFF;
            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }


    @Override
    public void start() {
    }

    @Override
    public void stop () {
    }

}
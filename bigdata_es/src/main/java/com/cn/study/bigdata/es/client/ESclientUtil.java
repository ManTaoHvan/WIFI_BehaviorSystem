package com.cn.shool.bigdata.bigdata.es.client;


import com.cn.shool.bigdata.bigdata.common.config.ConfigUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * @author:
 * @description:  获取ES客户端
 * @Date:Created in 2019-10-
 */
public class ESclientUtil {

    private static final Logger logger = Logger.getLogger(ESclientUtil.class);

    private static final String esConfigPath = "test/es/es_cluster.properties";

    private volatile static TransportClient client;

    private ESclientUtil(){};

    //ES配置
    private static Properties properties;

    //通过配置获取主机名和端口
    static {
        properties = ConfigUtil.getInstance().getProperties(esConfigPath);
    }
    //创建ES客户端
    public static TransportClient getClient(){
        String host1 = properties.get("es.cluster.nodes1").toString();
        String host2 = properties.get("es.cluster.nodes2").toString();
        String host3 = properties.get("es.cluster.nodes3").toString();
        Integer port = Integer.valueOf(properties.getProperty("es.cluster.tcp.port"));
        String myClusterName = properties.get("es.cluster.name").toString();

        if(client ==null){
            synchronized (ESclientUtil.class){
                if(client == null){
                    try {
                        //解决netty冲突
                       System.setProperty("es.set.netty.runtime.available.processors", "false");
                        Settings settings = Settings.builder()
                                .put("cluster.name", myClusterName).build();
                        client = new PreBuiltTransportClient(settings)
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(host1), port))
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(host2), port))
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(host3), port));

                    } catch (UnknownHostException e) {
                        logger.error("获取ES客户端失败",e);
                    }
                }
            }
        }
        return client;
    }
    public static void main(String[] args) {
        TransportClient instance = ESclientUtil.getClient();
        System.out.println(instance);
    }
}

package com.cn.shool.bigdata.bigdata.spark.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

import java.util.Iterator;
import java.util.Map;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class HiveConf {
    private static HiveContext hiveContext;

    public static HiveContext getHiveContext(SparkContext sparkContext){
        if(hiveContext ==null){
            synchronized (HiveConf.class){
                if(hiveContext ==null){

                    System.load("H:\\hadoop-common-2.6.0-bin-master\\bin\\hadoop.dll");
                    System.load("H:\\hadoop-common-2.6.0-bin-master\\bin\\winutils.exe");

                    hiveContext = new HiveContext(sparkContext);
                    //这里是要操作集群上的HIVE
                    Configuration conf = new Configuration();
                    conf.addResource("test/spark/hive/hive-site.xml");
                    conf.addResource("test/spark/hive/core-site.xml");
                    conf.addResource("test/spark/hive/hdfs-site.xml");
                    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
                    while (iterator.hasNext()){
                        Map.Entry<String, String> next = iterator.next();
                        hiveContext.setConf(next.getKey(),next.getValue());
                    }
                }
            }
        }
        return hiveContext;
    }
}

package com.cn.shool.bigdata.bigdata.redis.client;


import com.cn.shool.bigdata.bigdata.common.config.ConfigUtil;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;
import java.util.Properties;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class JedisUtil {

       private static final Logger LOG = Logger.getLogger(JedisUtil.class);

       private static Properties redisConf;

       private static final String redisConfPath = "test/redis/redis.properties";

       static {
              redisConf = ConfigUtil.getInstance().getProperties(redisConfPath);
       }

       public static Jedis getJedis(int db){

             Jedis jedis = JedisUtil.getJedis();
             if(jedis!=null){
                    jedis.select(db);
             }

             return jedis;
       }


       public static void close(Jedis jedis){
              if(jedis!=null){
                     jedis.close();
              }
       }

       /**
        * 并发很高的时候 会出现获取连接失败的情况  导致程序挂掉
        * @return
        */
       public static Jedis getJedis(){
              int timeoutCount = 0;
              while (true) {// 如果是网络超时则多试几次
                     try
                     {
                            Jedis jedis = new Jedis(redisConf.get("redis.hostname").toString(),
                                    Integer.valueOf(redisConf.get("redis.port").toString()));
                            return jedis;
                     } catch (Exception e)
                     {
                            if (e instanceof JedisConnectionException || e instanceof SocketTimeoutException)
                            {
                                   timeoutCount++;
                                   LOG.warn("获取jedis连接超时次数:" +timeoutCount);
                                   if (timeoutCount > 4)
                                   {
                                          LOG.error("获取jedis连接超时次数a:" +timeoutCount);
                                          LOG.error(null,e);
                                          break;
                                   }
                            }else
                            {
                                   LOG.error("getJedis error", e);
                                   break;
                            }
                     }
              }
              return null;
       }


       public static void main(String[] args) {
              Jedis jedis = JedisUtil.getJedis(10);
              jedis.hset("111","name","张三");
              jedis.hset("111","age","6");
              JedisUtil.close(jedis);
       }




}

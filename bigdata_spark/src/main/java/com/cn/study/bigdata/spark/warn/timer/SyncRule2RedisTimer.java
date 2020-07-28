package com.cn.shool.bigdata.bigdata.spark.warn.timer;

import akka.util.internal.Timer;
import com.cn.shool.bigdata.bigdata.redis.client.JedisUtil;
import com.cn.shool.bigdata.bigdata.spark.warn.dao.TZ_RuleDao;
import com.cn.shool.bigdata.bigdata.spark.warn.domain.TZ_RuleDomain;
import org.apache.calcite.util.Static;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.TimerTask;

/**
 * @author:
 * @description: 使用定时器将mysql中得规则同步到redis
 * @Date:Created in 2019-10-
 */
public class SyncRule2RedisTimer extends TimerTask {

    private static final Logger LOG = Logger.getLogger(SyncRule2RedisTimer.class);

    @Override
    public void run() {
        LOG.error("======开始同步mysql中的规则到redis======");
        //TODO 获取所有的规则
        List<TZ_RuleDomain> ruleList = TZ_RuleDao.getRuleList();
        //TODO 遍历所有规则 写入redis
        Jedis jedis = null;

        for (int i = 0; i <ruleList.size() ; i++) {
            try {
                jedis = JedisUtil.getJedis(15);
                TZ_RuleDomain rule = ruleList.get(i);
                //因为要把所有规则同步到redis,所以我们拿出所有字段
                String id = rule.getId()+"";      //主键
                String publisher = rule.getPublisher();//发布规则者
                String warn_fieldname = rule.getWarn_fieldname(); //告警字段名
                String warn_fieldvalue = rule.getWarn_fieldvalue(); //告警字段内容
                String send_mobile = rule.getSend_mobile();//获取接收电话
                String send_type = rule.getSend_type();

                //redisKEY 使用告警字段名 + ":" + 告警字段内容 作为比对的key
                String redisKey = warn_fieldname +":"+warn_fieldvalue;
                // 通过redis hash结构存储规则
                jedis.hset(redisKey,"id",StringUtils.isNoneBlank(id)?id:"");
                jedis.hset(redisKey,"publisher",StringUtils.isNoneBlank(publisher)?publisher:"");
                jedis.hset(redisKey,"warn_fieldname",StringUtils.isNoneBlank(warn_fieldname)?warn_fieldname:"");
                jedis.hset(redisKey,"warn_fieldvalue",StringUtils.isNoneBlank(warn_fieldvalue)?warn_fieldvalue:"");
                jedis.hset(redisKey,"send_mobile",StringUtils.isNoneBlank(send_mobile)?send_mobile:"");
                jedis.hset(redisKey,"sent_type",StringUtils.isNoneBlank(send_type)?send_type:"");

            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                JedisUtil.close(jedis);
            }
        }
        LOG.error("======同步规则成功=====" +ruleList.size());
    }

    public static void main(String[] args) {

        java.util.Timer timer = new java.util.Timer();
        timer.schedule(new SyncRule2RedisTimer(),0,1*10*1000);

    /*    SyncRule2RedisTimer syncRule2RedisTimer = new SyncRule2RedisTimer();
        syncRule2RedisTimer.run();*/
    }
}

package com.cn.shool.bigdata.bigdata.spark.warn.timer;

/**
 * @author:
 * @description: 消息发送工具类
 * @Date:Created in 2019-10-
 */
public class MessageSend {

    /**
     * 模拟短信接口
     * @param phone
     * @param sendInfo
     */
    public static void sendMessage(String phone,String sendInfo){
        System.out.println("发送短信到手机号为=》"+phone );
        System.out.println("发送内容为=========》"+sendInfo );
    }


    /**
     * 模拟短信接口
     * @param phone
     * @param sendInfo
     */
    public static void sendDingDingMessage(String phone,String sendInfo){
        System.out.println("发送消息到钉钉号为=》"+phone );
        System.out.println("发送内容为=========》"+sendInfo );
    }
}

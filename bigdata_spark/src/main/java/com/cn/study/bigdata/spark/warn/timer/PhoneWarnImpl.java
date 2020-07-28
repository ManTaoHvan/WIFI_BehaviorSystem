package com.cn.shool.bigdata.bigdata.spark.warn.timer;

import com.cn.shool.bigdata.bigdata.spark.warn.domain.WarningMessage;

/**
 * @author:
 * @description: 手机短信告警
 * @Date:Created in 2019-10-
 */
public class PhoneWarnImpl implements WarnI {
    @Override
    public boolean warn(WarningMessage warningMessage) {
        //现从WarningMessage 获取接收方式
        String sendInfo = warningMessage.getSenfInfo();
        String[] sendPhones = warningMessage.getSendMobile().split(",");
        //遍历手机号，调用第三方短信接口
        for (int i = 0; i <sendPhones.length ; i++) {
              String phone =  sendPhones[i];
              //调用第三方接口
             MessageSend.sendMessage(phone,sendInfo);
        }
        return true;
    }



}

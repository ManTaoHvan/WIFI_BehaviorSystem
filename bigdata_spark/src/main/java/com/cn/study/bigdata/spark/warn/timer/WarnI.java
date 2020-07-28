package com.cn.shool.bigdata.bigdata.spark.warn.timer;

import com.cn.shool.bigdata.bigdata.spark.warn.domain.WarningMessage;

/**
 * @author:
 * @description: 告警接口
 * @Date:Created in 2019-10-
 */
public interface WarnI {

      boolean warn(WarningMessage warningMessage);
}

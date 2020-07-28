package com.cn.shool.bigdata.bigdata.spark.common.convert;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class BaseDataConvert {

    private static final Logger LOG = LoggerFactory.getLogger(BaseDataConvert.class);

    public static HashMap<String,Object> mapString2Long(Map<String,String> map, String key, HashMap<String,Object> objectMap) {
        String logouttime = map.get(key);
        if (StringUtils.isNotBlank(logouttime)) {
            objectMap.put(key, Long.valueOf(logouttime));
        } else {
            objectMap.put(key, 0L);
        }
        return objectMap;
    }


    public static HashMap<String,Object> mapString2Double(Map<String,String> map, String key, HashMap<String,Object> objectMap) {
        String logouttime = map.get(key);
        if (StringUtils.isNotBlank(logouttime)) {
            objectMap.put(key, Double.valueOf(logouttime));
        } else {
            objectMap.put(key, 0.000000);
        }
        return objectMap;
    }


    public static HashMap<String,Object> mapString2String(Map<String,String> map, String key, HashMap<String,Object> objectMap) {
        String logouttime = map.get(key);
        if (StringUtils.isNotBlank(logouttime)) {
            objectMap.put(key, logouttime);
        } else {
            objectMap.put(key, "");
        }
        return objectMap;
    }

}

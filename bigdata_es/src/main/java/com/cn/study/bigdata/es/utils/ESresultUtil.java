package com.cn.shool.bigdata.bigdata.es.utils;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class ESresultUtil {

    private static Logger LOG = LoggerFactory.getLogger(ESresultUtil.class);

    public static Long getLong(Map<String,Object> esMAp,String field){

        Long valueLong = 0L;
        if(esMAp!=null && esMAp.size()>0){
            if(esMAp.containsKey(field)){
                 Object value = esMAp.get(field);
                 if(value!=null && StringUtils.isNotBlank(value.toString())){
                     valueLong = Long.valueOf(value.toString());
                 }
            }
        }
        return valueLong;
    }


}

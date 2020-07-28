package com.cn.shool.bigdata.bigdata.common.properties;

import com.cn.shool.bigdata.bigdata.common.config.ConfigUtil;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author:
 * @description: 封装DataTypeProperties的属性
 * @Date:Created in 2019-10-
 */
public class DataTypeProperties {
    private static final Logger LOG = Logger.getLogger(DataTypeProperties.class);

    private static final String DATA_PATH = "test/common/datatype.properties";

    public static Map<String,ArrayList<String>> dataTypeMap = null;

    static {
        Properties properties = ConfigUtil.getInstance().getProperties(DATA_PATH);
        dataTypeMap = new HashMap<>();
        Set<Object> keys = properties.keySet();
        keys.forEach(key->{
            String[] fields = properties.getProperty(key.toString()).split(",");
            dataTypeMap.put(key.toString(),new ArrayList<>(Arrays.asList(fields)));
        });
    }

    public static void main(String[] args) {
        Map<String, ArrayList<String>> dataTypeMap = DataTypeProperties.dataTypeMap;
        dataTypeMap.keySet().forEach(key->{
            System.out.println(key);
        });
    }


}

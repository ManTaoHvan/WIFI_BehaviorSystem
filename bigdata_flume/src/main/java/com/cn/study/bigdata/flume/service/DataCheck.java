package com.cn.shool.bigdata.bigdata.flume.service;

import com.cn.shool.bigdata.bigdata.common.properties.DataTypeProperties;
import com.cn.shool.bigdata.bigdata.flume.constant.MapFields;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class DataCheck {

    private static final Logger LOG = Logger.getLogger(DataCheck.class);

    public static Map<String,ArrayList<String>> dataTypeMap;

    static {
        dataTypeMap = DataTypeProperties.dataTypeMap;
    }

    /**
     *  清洗
     * @param fileName                文件名，通过文件名识别数据类型
     * @param absolute_filename
     * @param line
     */
    public static Map<String,String> txtParseAndValidation(String fileName,
                                             String absolute_filename,
                                             String line){

        Map map = new HashMap<String,String>(); //存正确数据
        Map errorMap = new HashMap<String,String>(); //存放错误信息
        //TODO string转map

        //wechat_source1_1111193.txt  数据类型_来源_UUID.txt
        String table = fileName.split("_")[0];
        //拿到某一类数据的所有字段
        ArrayList<String> fields = dataTypeMap.get(table);
        //fields = imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,username,phone,object_username,send_message,accept_message,message_time
        //000000000000000	000000000000000	23.000000	24.000000	aa-aa-aa-aa-aa-aa	bb-bb-bb-bb-bb-bb	32109231	1557305988	andiy	18609765432	judy			1789098762
        String[] values = line.split("\t");


        //TODO  校验   正确的，错误
        //判断长度是不是一样长
        if(fields.size() == values.length){
            for (int i = 0; i < values.length; i++) {
                map.put(fields.get(i),values[i]);
            }
            //数据加工，数据信息补全
            //补全唯一ID
            map.put(MapFields.ID,UUID.randomUUID().toString().replace("-",""));


            map.put(MapFields.TABLE,table);
            map.put(MapFields.RKSJ,(System.currentTimeMillis()/1000)+"");
            map.put(MapFields.FILENAME,fileName);
            map.put(MapFields.ABSOLUTE_FILENAME,absolute_filename);
        }else{
            //TODO 错误处理
            // 记录错误信息，方便错误排查，这个结果我们会保存到ES中
            errorMap.put("leng_error","字段和值的长度部匹配");
            errorMap.put("leng","字段数不匹配,需要的长度为" + fields.size() + "\t" + "实际是" + values.length);
        }
        //数据清洗的必须性
        //1.导致程序异常
        //2.业务功能不可用
        //3.数据挖掘模型准确率低   //一个模型，前期优化可以通过各种方式去提升学习准确率，
        //达到一个定点之后，无论怎么优化，都不可能再提升准确率，准确率的高低就是通过数据质量来决定
        //4.错误数据监控
        //TODO 继续对解析之后的 map 进行校验清洗

        if(map!=null && map.size()>0){
            // 汇集所有的错误信息
            errorMap = DataValidation.dataValidation(map);

        }
        //TODO  将errorMap 写入到ES

        if(errorMap.size()>0){
            //已经产生错误数据，说明这条数据已经错误，没必要写入数据库中了
            map = null;
        }

        return map;
    }


}

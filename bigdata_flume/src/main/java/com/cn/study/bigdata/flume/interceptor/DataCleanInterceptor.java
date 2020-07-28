package com.cn.shool.bigdata.bigdata.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.cn.shool.bigdata.bigdata.common.properties.DataTypeProperties;
import com.cn.shool.bigdata.bigdata.flume.constant.ConstantFields;
import com.cn.shool.bigdata.bigdata.flume.service.DataCheck;
import org.apache.commons.io.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author:
 * @description:  自定义拦截器
 * @Date:Created in 2019-10-
 */
public class DataCleanInterceptor implements Interceptor {
    
    private static final Logger LOG = Logger.getLogger(DataCleanInterceptor.class);
    @Override
    public void initialize() {
        
    }

    //拦截
    @Override
    public Event intercept(Event event) {
        //直接在这里定义拦截业务

        //TODO string转map
        if(event == null){
             return null;
        }

        Event eventNew = new SimpleEvent();
        //将event钟的数据解析出来
        Map<String, String> headers = event.getHeaders();
        String fileName = headers.get(ConstantFields.FILENAME);
        String absolute_filename = headers.get(ConstantFields.ABSOLUTE_FILENAME);

        //imei,imsi,longitude,latitude,phone_mac,device_mac,device_number,collect_time,username,phone,object_username,send_message,accept_message,message_time
        //000000000000000	000000000000000	23.000000	24.000000	aa-aa-aa-aa-aa-aa	bb-bb-bb-bb-bb-bb	32109231	1557305988	andiy	18609765432	judy			1789098762
        String line = new String(event.getBody(),Charsets.UTF_8);

        //TODO 单独定义一个工具类对数据进行清洗
        Map<String, String> map = DataCheck.txtParseAndValidation(fileName, absolute_filename, line);

        if(map==null){
            return null;
        }

        String json = JSON.toJSONString(map);
        eventNew.setBody(json.getBytes());
        //TODO  数据清洗
        return eventNew;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> listEvents = new ArrayList<>();
        events.forEach(event->{
            Event eventNew = intercept(event);
            listEvents.add(eventNew);
        });
        return listEvents;
    }


    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new DataCleanInterceptor();
        }
        @Override
        public void configure(Context context) {

        }
    }

    @Override
    public void close() {

    }
}

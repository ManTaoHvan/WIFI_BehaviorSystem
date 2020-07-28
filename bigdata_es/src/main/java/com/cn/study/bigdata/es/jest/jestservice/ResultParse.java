package com.cn.shool.bigdata.bigdata.es.jest.jestservice;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author:
 * @description: ES结果解析  跟HABSE ROW解析相比较
 * @Date:Created in 2019-10-
 */
public class ResultParse {
    private static Logger LOG = LoggerFactory.getLogger(ResultParse.class);



    public static void main(String[] args) throws Exception {
        JestClient jestClient = JestService.getJestClient();


        /*long l = System.currentTimeMillis();
        JestClient jestClient = JestClientUtil.getJestClient();
        System.out.println(jestClient);
        String json ="{\n" +
                "  \"size\": 1, \n" +
                "  \"query\": {\n" +
                "    \"query_string\": {\n" +
                "      \"query\": \"中文\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"highlight\": {\n" +
                "    \"pre_tags\" : [ \"<red>\" ],\n" +
                "    \"post_tags\" : [ \"</red>\" ],\n" +
                "    \"fields\":{\n" +
                "      \"secondlanguage\": {}\n" +
                "      ,\"firstlanguage\": {}\n" +
                "    }\n" +
                "  }\n" +
                "}";
        SearchResult search = JestService.search(jestClient, ES_INDEX.TANSLATOR_TEST, ES_INDEX.TANSLATOR_TEST,json);
        ResultParse.parseSearchResult(search);
        jestClient.shutdownClient();
        long l1 = System.currentTimeMillis();
        System.out.println(l1-l);*/
    }

    /**
     * 继续ES查询出来的 JestResult 结果
     * @param getResult
     * @return
     */
    public static Map<String,Object> parseGet(JestResult getResult){
        Map<String,Object> map = null;
        JsonObject jsonObject = getResult.getJsonObject().getAsJsonObject("_source");
        if(jsonObject != null){
            map = new HashMap<String,Object>();
            //System.out.println(jsonObject);
            Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();
            for(Map.Entry<String, JsonElement> entry:entries){
                JsonElement value = entry.getValue();
                if(value.isJsonPrimitive()){
                    JsonPrimitive value1 = (JsonPrimitive) value;
                    map.put(entry.getKey(),value1.getAsString());

                /*    if( value1.isString() ){


                    }else{

                        map.put(entry.getKey(),value1);
                    }*/
                }else{
                    map.put(entry.getKey(),value);
                }
             }
        }
        return map;
    }

    public static Map<String,Object> parseGet2map(JestResult getResult){

        JsonObject source = getResult.getJsonObject().getAsJsonObject("_source");
        Gson gson = new Gson();
        Map map = gson.fromJson(source, Map.class);
        return map;
    }

    /**
     * 解析listMap
     * 结果格式为  {hits=0, total=0, data=[]}
     * @param search
     * @return
     */
    public static List<Map<String,Object>> parseSearchResultOnly(SearchResult search){

        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        List<SearchResult.Hit<Object, Void>> hits = search.getHits(Object.class);
        for(SearchResult.Hit<Object, Void> hit : hits){
            Map<String,Object> source = (Map<String,Object>)hit.source;
            list.add(source);
        }
        return list;
    }


    /**
     * 解析listMap
     * 结果格式为  {hits=0, total=0, data=[]}
     * @param search
     * @return
     */
    public static Map<String,Long> parseAggregation(SearchResult search){
        Map<String,Long> mapResult = new HashMap<>();
        MetricAggregation aggregations = search.getAggregations();
        TermsAggregation group1 = aggregations.getTermsAggregation("group1");
        List<TermsAggregation.Entry> buckets = group1.getBuckets();
        buckets.forEach(x->{
            String key = x.getKey();
            Long count = x.getCount();
            mapResult.put(key,count);
        });
        return mapResult;
    }



    /**
     * 解析listMap
     * 结果格式为  {hits=0, total=0, data=[]}
     * @param search
     * @return
     */
    public static Map<String,Object> parseSearchResult(SearchResult search){

        Map<String,Object> map = new HashMap<String,Object>();
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();

        Long total = search.getTotal();
        map.put("total",total);
        List<SearchResult.Hit<Object, Void>> hits = search.getHits(Object.class);
        map.put("hits",hits.size());
        for(SearchResult.Hit<Object, Void> hit : hits){
            Map<String, List<String>> highlight = hit.highlight;
            Map<String,Object> source = (Map<String,Object>)hit.source;
            source.put("highlight",highlight);
            list.add(source);
        }
        map.put("data",list);
        return map;
    }


}

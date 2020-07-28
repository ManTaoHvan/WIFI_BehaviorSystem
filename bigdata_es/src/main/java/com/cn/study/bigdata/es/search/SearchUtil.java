package com.cn.shool.bigdata.bigdata.es.search;

import com.cn.shool.bigdata.bigdata.es.client.ESclientUtil;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author: chenhailong
 * @description:
 * @Date:Created in 2019-10-
 */
public class SearchUtil {

    private static Logger LOG = LoggerFactory.getLogger(SearchUtil.class);


    private static TransportClient client = ESclientUtil.getClient();

    public static void main(String[] args) {
        TransportClient client = ESclientUtil.getClient();
        List<Map<String, Object>> maps = searchSingleData(client, "wechat", "wechat", "phone_mac", "aa-aa-aa-aa-aa-aa");
        System.out.println(maps);
  /*      long l = System.currentTimeMillis();
        searchSingleData("tanslator", "tanslator","4e1117d7-c434-48a7-9134-45f7c90f94ee_TR1100397895_2");
        System.out.println("消耗时间" + (System.currentTimeMillis() - l));

        long lll = System.currentTimeMillis();
        searchSingleData("tanslator", "tanslator","4e1117d7-c434-48a7-9134-45f7c90f94ee_TR1100397895_2");
        System.out.println("消耗时间" + (System.currentTimeMillis() - lll));

        long ll = System.currentTimeMillis();
        List<Map<String, Object>> maps = searchSingleData(client,"tanslator", "tanslator", "iolid", "TR1100397895");
        System.out.println("消耗时间" + (System.currentTimeMillis() - ll));
        System.out.println(maps);*/
    }

    /**
     * 查询单条数据
     * @param index  索引
     * @param type   表名
     * @param id     字段
     * @return
     */
    public static GetResponse searchSingleData(String index, String type, String id) {
        GetResponse response = null;
        try {
            GetRequestBuilder builder = null;
            builder = client.prepareGet(index, type, id);
            response = builder.execute().actionGet();
        } catch (Exception e) {
            LOG.error(null, e);
        }
        return response;
    }

    /**
     *
     * @param index
     * @param type
     * @param field
     * @param value
     * @return
     */
    public static List<Map<String, Object>> searchSingleData(TransportClient client,String index, String type,String field, String value) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            SearchRequestBuilder builder = BuilderUtil.getSearchBuilder(client,index,type);
            MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, value);
            builder.setQuery(matchQueryBuilder).setExplain(false);
            SearchResponse searchResponse = builder.execute().actionGet();
            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHists = hits.getHits();
            for (SearchHit sh : searchHists) {
                result.add(sh.getSourceAsMap());
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(null, e);
        }
        return result;
    }

    /**
     * 多條件查詢
     * @param index
     * @param type
     * @param paramMap 組合查詢條件
     * @return
     */
    public static SearchResponse searchListData(String index, String type,
                                                Map<QueryUtil.OPREATOR,Map<String,Object>> paramMap) {

        SearchRequestBuilder builder = BuilderUtil.getSearchBuilder(client,index,type);
        builder.setQuery(QueryUtil.getSearchParam(paramMap)).setExplain(false);
        SearchResponse searchResponse = builder.get();

        return searchResponse;
    }


    /**
     * 多條件查詢
     * @param index
     * @param type
     * @param paramMap 組合查詢條件
     * @return
     */
    public static SearchResponse searchListData1(String index, String type, Map<String,String> paramMap) {

        BoolQueryBuilder qb = QueryBuilders.boolQuery();
        qb.must(QueryBuilders.matchQuery("", ""));

        BoolQueryBuilder qb1 = QueryBuilders.boolQuery();
        qb1.should(QueryBuilders.matchQuery("",""));
        qb1.should(QueryBuilders.matchQuery("",""));

        qb.must(qb1);
        return null;
    }




}

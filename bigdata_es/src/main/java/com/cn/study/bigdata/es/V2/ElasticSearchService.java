package com.cn.shool.bigdata.bigdata.es.V2;

import com.cn.shool.bigdata.bigdata.es.client.ESclientUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

/**
 * @author: 
 * @description: ES检索封装
 * @Date:Created in 2019-10-
 */
public class ElasticSearchService {
    private final static int MAX = 10000;
    private static TransportClient client = ESclientUtil.getClient();
    /**
     * 功能描述：新建索引
     * @param indexName 索引名
     */
    public void createIndex(String indexName) {
        client.admin().indices().create(new CreateIndexRequest(indexName))
                .actionGet();
    }

    /**
     * 功能描述：新建索引
     * @param index 索引名
     * @param type 类型
     */
    public void createIndex(String index, String type) {
        client.prepareIndex(index, type).setSource().get();
    }

    /**
     * 功能描述：删除索引
     * @param index 索引名
     */
    public void deleteIndex(String index) {
        if (indexExist(index)) {
            DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index)
                    .execute().actionGet();
            if (!dResponse.isAcknowledged()) {

            }
        } else {

        }
    }

    /**
     * 功能描述：验证索引是否存在
     * @param index 索引名
     */
    public boolean indexExist(String index) {
        IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(index);
        IndicesExistsResponse inExistsResponse = client.admin().indices()
                .exists(inExistsRequest).actionGet();
        return inExistsResponse.isExists();
    }

    /**
     * 功能描述：插入数据
     * @param index 索引名
     * @param type 类型
     * @param json 数据
     */
    public void insertData(String index, String type, String json) {
       client.prepareIndex(index, type)
                .setSource(json)
                .get();
    }

    /**
     * 功能描述：插入数据
     * @param index 索引名
     * @param type 类型
     * @param _id 数据id
     * @param json 数据
     */
    public void insertData(String index, String type, String _id, String json) {
        client.prepareIndex(index, type).setId(_id)
                .setSource(json)
                .get();
    }

    /**
     * 功能描述：更新数据
     * @param index 索引名
     * @param type 类型
     * @param _id 数据id
     * @param json 数据
     */
    public void updateData(String index, String type, String _id, String json) throws Exception {
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, type, _id)
                    .doc(json);
            client.update(updateRequest).get();
        } catch (Exception e) {
            //throw new MessageException("update data failed.", e);
        }
    }

    /**
     * 功能描述：删除数据
     * @param index 索引名
     * @param type 类型
     * @param _id 数据id
     */
    public void deleteData(String index, String type, String _id) {
        client.prepareDelete(index, type, _id)
                .get();
    }

    /**
     * 功能描述：批量插入数据
     * @param index 索引名
     * @param type 类型
     * @param data (_id 主键, json 数据)
     */
    public void bulkInsertData(String index, String type, Map<String, String> data) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        data.forEach((param1, param2) -> {
            bulkRequest.add(client.prepareIndex(index, type, param1)
                    .setSource(param2)
            );
        });
        bulkRequest.get();
    }

    /**
     * 功能描述：批量插入数据
     * @param index 索引名
     * @param type 类型
     * @param jsonList 批量数据
     */
    public void bulkInsertData(String index, String type, List<String> jsonList) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        jsonList.forEach(item -> {
            bulkRequest.add(client.prepareIndex(index, type)
                    .setSource(item)
            );
        });
        bulkRequest.get();
    }

    /**
     * 功能描述：查询
     * @param index 索引名
     * @param type 类型
     * @param constructor 查询构造
     */
    public List<Map<String, Object>> search(String index, String type, ESQueryBuilderConstructor constructor) {

        List<Map<String, Object>> list = new ArrayList<>();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //排序
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        //设置查询体
        searchRequestBuilder.setQuery(constructor.listBuilders());
        //返回条目数
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);
        searchRequestBuilder.setFrom(constructor.getFrom() < 0 ? 0 : constructor.getFrom());
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for (SearchHit sh : searchHists) {
            list.add(sh.getSourceAsMap());
        }

        return list;
    }


    /**
     * 功能描述：查询
     * @param index 索引名
     * @param type 类型
     * @param constructor 查询构造
     */
    public Map<String,Object> searchCountAndMessage(String index, String type, ESQueryBuilderConstructor constructor) {
        Map<String,Object> map = new HashMap<String,Object>();
        List<Map<String, Object>> list = new ArrayList<>();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //排序
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        //设置查询体
        searchRequestBuilder.setQuery(constructor.listBuilders());
        //返回条目数
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);
        searchRequestBuilder.setFrom(constructor.getFrom() < 0 ? 0 : constructor.getFrom());
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
        long totalHits = searchResponse.getHits().getTotalHits();

        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for (SearchHit sh : searchHists) {
            list.add(sh.getSourceAsMap());
        }
        map.put("total",(long)searchHists.length);
        map.put("count",totalHits);
        map.put("data",list);
        return map;
    }


    /**
     * 功能描述：查询
     * @param index 索引名
     * @param type 类型
     * @param constructor 查询构造
     */
    public Map<String,Object> searchCountAndMessageNew(String index, String type, ESQueryBuilderConstructorNew constructor) {
        Map<String,Object> map = new HashMap<String,Object>();
        List<Map<String, Object>> list = new ArrayList<>();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //排序
        List<SortBuilder> sortBuilderList = constructor.getSortBuilderList();
        if(sortBuilderList!=null && sortBuilderList.size()>0){
            sortBuilderList.forEach(sortBuilder->{
                searchRequestBuilder.addSort(sortBuilder);
            });
        }


        //设置查询体
        searchRequestBuilder.setQuery(constructor.listBuilders());
        //返回条目数
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);
        searchRequestBuilder.setFrom(constructor.getFrom() < 0 ? 0 : constructor.getFrom());

        //设置高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        List<String> highLighterFields = constructor.getHighLighterFields();
        if(highLighterFields.size()>0){
            highLighterFields.forEach(field -> {
                highlightBuilder.field(field);
            });

        }
        highlightBuilder.preTags("<font color=\"red\">");
        highlightBuilder.postTags("</font>");
        SearchResponse searchResponse = searchRequestBuilder.highlighter(highlightBuilder).execute().actionGet();
        long totalHits = searchResponse.getHits().getTotalHits();

        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHists = hits.getHits();
        for (SearchHit hit : searchHists) {

            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();

            //获取高亮结果
            Set<String> set = highlightFields.keySet();

            for (String str : set) {
                Text[] fragments = highlightFields.get(str).getFragments();
                String st1r="";
                for(Text text:fragments){
                    st1r = st1r + text.toString();
                }
                sourceAsMap.put(str,st1r);
                System.out.println("str(==============" + st1r);
            }

            list.add(sourceAsMap);
        }
        map.put("total",(long)searchHists.length);
        map.put("count",totalHits);
        map.put("data",list);
        return map;
    }
    /**
     * 功能描述：统计查询
     * @param index 索引名
     * @param type 类型
     * @param constructor 查询构造
     * @param groupBy 统计字段
     */
    public Map<Object, Object> statSearch(String index, String type, ESQueryBuilderConstructor constructor, String groupBy) {
        Map<Object, Object> map = new HashedMap();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //排序
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        //设置查询体
        if (null != constructor) {
            searchRequestBuilder.setQuery(constructor.listBuilders());
        } else {
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);

        searchRequestBuilder.setFrom(constructor.getFrom() < 0 ? 0 : constructor.getFrom());
        SearchResponse sr = searchRequestBuilder.addAggregation(
                AggregationBuilders.terms("agg").field(groupBy)
        ).get();

        Terms stateAgg = sr.getAggregations().get("agg");

        Iterator<? extends Terms.Bucket> iter = stateAgg.getBuckets().iterator();

        while (iter.hasNext()) {
            Terms.Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey(), gradeBucket.getDocCount());
        }

        return map;
    }

    /**
     * 功能描述：统计查询
     * @param index 索引名
     * @param type 类型
     * @param constructor 查询构造
     * @param agg 自定义计算
     */
    public Map<Object, Object> statSearch(String index, String type, ESQueryBuilderConstructor constructor, AggregationBuilder agg) {
        if (agg == null) {
            return null;
        }
        Map<Object, Object> map = new HashedMap();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type);
        //排序
        if (StringUtils.isNotEmpty(constructor.getAsc()))
            searchRequestBuilder.addSort(constructor.getAsc(), SortOrder.ASC);
        if (StringUtils.isNotEmpty(constructor.getDesc()))
            searchRequestBuilder.addSort(constructor.getDesc(), SortOrder.DESC);
        //设置查询体
        if (null != constructor) {
            searchRequestBuilder.setQuery(constructor.listBuilders());
        } else {
            searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        }
        int size = constructor.getSize();
        if (size < 0) {
            size = 0;
        }
        if (size > MAX) {
            size = MAX;
        }
        //返回条目数
        searchRequestBuilder.setSize(size);

        searchRequestBuilder.setFrom(constructor.getFrom() < 0 ? 0 : constructor.getFrom());
        SearchResponse sr = searchRequestBuilder.addAggregation(
                agg
        ).get();

        Terms stateAgg = sr.getAggregations().get("agg");
        Iterator<? extends Terms.Bucket> iter = stateAgg.getBuckets().iterator();

        while (iter.hasNext()) {
            Terms.Bucket gradeBucket = iter.next();
            map.put(gradeBucket.getKey(), gradeBucket.getDocCount());
        }

        return map;
    }


    /**
     * 功能描述：关闭链接
     */
    public void close() {
        client.close();
    }

    public static void test() {
        try{
            ElasticSearchService service = new ElasticSearchService();
            ESQueryBuilderConstructorNew constructor = new ESQueryBuilderConstructorNew();
            constructor.must(new ESQueryBuilders().bool(QueryBuilders.boolQuery()));
            constructor.must(new ESQueryBuilders().match("secondlanguage", "4"));
            constructor.must(new ESQueryBuilders().match("secondlanguage", "4"));
            constructor.should(new ESQueryBuilders().match("source", "5"));
            constructor.should(new ESQueryBuilders().match("source", "5"));
            service.searchCountAndMessageNew("", "", constructor);
        }catch (Exception e){
            e.printStackTrace();
        }
    }





    public static void main(String[] args) {
        try {

            ElasticSearchService service = new ElasticSearchService();
            ESQueryBuilderConstructor constructor = new ESQueryBuilderConstructor();
            constructor.must(new ESQueryBuilders().match("phone_mac", "aa-aa-aa-aa-aa-aa"));
            List<Map<String, Object>> list = service.search("wechat", "wechat", constructor);
            for(Map<String, Object> map : list){
                System.out.println(map);
            }



         /*   constructor.must(new ESQueryBuilders().term("gender", "f").range("age", 20, 50));

            constructor.should(new ESQueryBuilders().term("gender", "f").range("age", 20, 50).fuzzy("age", 20));
            constructor.mustNot(new ESQueryBuilders().term("gender", "m"));
            constructor.setSize(15);  //查询返回条数，最大 10000
            constructor.setFrom(11);  //分页查询条目起始位置， 默认0
            constructor.setAsc("age"); //排序

            List<Map<String, Object>> list = service.search("bank", "account", constructor);
            Map<Object, Object> map = service.statSearch("bank", "account", constructor, "state");*/
           /* ElasticSearchService service = new ElasticSearchService();
            ESQueryBuilderConstructor constructor = new ESQueryBuilderConstructor();
            constructor.must(new ESQueryBuilders().match("id", "WE16000190TR"));
            List<Map<String, Object>> list = service.search("test01", "test01", constructor);
             for(Map<String, Object> map : list){
                 System.out.println(map);
             }*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

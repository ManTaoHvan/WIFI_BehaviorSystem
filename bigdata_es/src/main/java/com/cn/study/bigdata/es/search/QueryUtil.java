package com.cn.shool.bigdata.bigdata.es.search;

import com.cn.shool.bigdata.bigdata.es.utils.UnicodeUtil;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author: chenhailong
 * @description:
 * @Date:Created in 2019-10-
 */
public class QueryUtil {

    private static Logger LOG = LoggerFactory.getLogger(QueryUtil.class);

    /**
     * EQ   等於
     * NEQ  不等於
     * GE   大于等于
     * GT   大于
     * LE   小于等于
     * LT   小于
     * RANGE 区间范围
     */
    public static enum OPREATOR {EQ, NEQ,WILDCARD, GE, LE, GT, LT, FUZZY, RANGE, IN, PREFIX}

    /**
     * @param paramMap
     * @return
     */
    public static BoolQueryBuilder getSearchParam(Map<OPREATOR, Map<String, Object>> paramMap) {

        BoolQueryBuilder qb = QueryBuilders.boolQuery();

        if (null != paramMap && !paramMap.isEmpty()) {

            for (Map.Entry<OPREATOR, Map<String, Object>> paramEntry : paramMap.entrySet()) {

                OPREATOR key = paramEntry.getKey();
                Map<String, Object> fieldMap = paramEntry.getValue();

                for (Map.Entry<String, Object> fieldEntry : fieldMap.entrySet()) {

                    String field = fieldEntry.getKey();
                    Object value = fieldEntry.getValue();

                    switch (key) {
                        case EQ:/**等於查詢 equale**/
                            qb.must(QueryBuilders.matchPhraseQuery(field, value).slop(0));
                            break;
                        case NEQ:/**不等於查詢 not equale**/
                            qb.mustNot(QueryBuilders.matchQuery(field, value));
                            break;
                        case GE: /**大于等于查詢  great than or equal to**/
                            qb.must(QueryBuilders.rangeQuery(field).gte(value));
                            break;
                        case LE: /**小于等于查詢 less than or equal to**/
                            qb.must(QueryBuilders.rangeQuery(field).lte(value));
                            break;
                        case GT: /**大于查詢**/
                            qb.must(QueryBuilders.rangeQuery(field).gt(value));
                            break;
                        case LT: /**小于查詢**/
                            qb.must(QueryBuilders.rangeQuery(field).lt(value));
                            break;
                        case FUZZY:
                            String text = String.valueOf(value);
                            if (!UnicodeUtil.hasChinese(text)) {
                                text = "*" + text + "*";
                            }
                            text = QueryParser.escape(text);
                            qb.must(new QueryStringQueryBuilder(text).field(field));
                            break;

                        case RANGE: /**区间查詢**/
                            String[] split = value.toString().split(",");
                            if(split.length==2){
                                qb.must(QueryBuilders.rangeQuery(field).from(Long.valueOf(split[0]))
                                        .to(Long.valueOf(split[1])));
                            }
 /*                           if (value instanceof Map) {
                                Map<String, Object> rangMap = (Map<String, Object>) value;
                                qb.must(QueryBuilders.rangeQuery(field).from(rangMap.get("ge"))
                                        .to(rangMap.get("le")));
                            }*/
                            break;

                        case PREFIX: /**前缀查詢**/
                            qb.must(QueryBuilders.prefixQuery(field, String.valueOf(value)));
                            break;

                        case IN:
                            qb.must(QueryBuilders.termsQuery(field, (Object[]) value));
                            break;

                        default:
                            qb.must(QueryBuilders.matchQuery(field, value));
                            break;
                    }
                }
            }
        }
        return qb;
    }
}

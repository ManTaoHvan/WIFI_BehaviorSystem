package com.cn.shool.bigdata.bigdata.es.V2;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Collection;

import static org.elasticsearch.index.search.MatchQuery.Type.PHRASE;

/**
 * @author: ""
 * @description: 条件表达式
 * @Date:Created in 2019-10-
 */
public class ESSimpleExpression {
    private String[] fieldNames;         //属性名
    private String fieldName;         //属性名
    private Object value;             //对应值
    private Collection<Object> values;//对应值
    private ESCriterion.Operator operator;        //计算符
    private Object from;
    private Object to;

    protected  ESSimpleExpression() {
    }

    protected  ESSimpleExpression(Object value, ESCriterion.Operator operator, String... fieldNames) {
        this.fieldNames = fieldNames;
        this.value = value;
        this.operator = operator;
    }


    protected  ESSimpleExpression(String fieldName, Object value, ESCriterion.Operator operator) {
        this.fieldName = fieldName;
        this.value = value;
        this.operator = operator;
    }

    protected  ESSimpleExpression(String value, ESCriterion.Operator operator) {
        this.value = value;
        this.operator = operator;
    }

    protected ESSimpleExpression(String fieldName, Collection<Object> values) {
        this.fieldName = fieldName;
        this.values = values;
        this.operator = ESCriterion.Operator.TERMS;
    }

    protected ESSimpleExpression(String fieldName, Object from, Object to) {
        this.fieldName = fieldName;
        this.from = from;
        this.to = to;
        this.operator = ESCriterion.Operator.RANGE;
    }

    public BoolQueryBuilder toBoolQueryBuilder(){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("",""));
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("",""));

        return null;
    }



    public QueryBuilder toBuilder() {
        QueryBuilder qb = null;
        switch (operator) {
            case MATCH:
                qb = QueryBuilders.matchQuery(fieldName, value);
                break;
            case MATCH_PHRASE:
                qb = QueryBuilders.matchPhraseQuery(fieldName, value);
                break;
            case MULTI_MATCH:
                qb = QueryBuilders.multiMatchQuery(value,fieldNames).type(PHRASE);
                break;
            case TERM:
                qb = QueryBuilders.termQuery(fieldName, value);
                break;
            case TERMS:
                qb = QueryBuilders.termsQuery(fieldName, values);
                break;
            case RANGE:
                qb = QueryBuilders.rangeQuery(fieldName).from(from).to(to).includeLower(true).includeUpper(true);
                break;
            case GTE:
                qb = QueryBuilders.rangeQuery(fieldName).gte(value);
                break;
            case LTE:
                qb = QueryBuilders.rangeQuery(fieldName).lte(value);
                break;

            case FUZZY:
                qb = QueryBuilders.fuzzyQuery(fieldName, value);
                break;
            case PREFIX:
                qb = QueryBuilders.prefixQuery(fieldName, value.toString());
                break;
            case QUERY_STRING:
                qb = QueryBuilders.queryStringQuery(value.toString());
                default:
        }
        return qb;
    }
}

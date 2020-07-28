package com.cn.shool.bigdata.bigdata.es.V2;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author: ""
 * @description: 条件构造器
 * @Date:Created in 2019-10-
 */
public class ESQueryBuilders implements ESCriterion{

    private List<QueryBuilder> list = new ArrayList<QueryBuilder>();


    /**
     * 功能描述：match 查询
     * @param field 字段名
     * @param value 值
     */
    public ESQueryBuilders match(String field, Object value) {
        list.add(new ESSimpleExpression (field, value, Operator.MATCH).toBuilder());
        return this;
    }

    /**
     * 功能描述：match 查询
     * @param field 字段名
     * @param value 值
     */
    public ESQueryBuilders match_phrase(String field, Object value) {
        list.add(new ESSimpleExpression (field, value, Operator.MATCH_PHRASE).toBuilder());
        return this;
    }

/*
    */
/**
     * 功能描述：match 查询
     * @param fieldNames 字段名
     * @param value 值
     *//*

    public ESQueryBuilders multi_match(Object value , String... fieldNames ) {
        String[] fields = fieldNames;
        list.add(new ESSimpleExpression(value, Operator.MULTI_MATCH,fields).toBuilder());
        return this;
    }
*/



    /**
     * 功能描述：Term 查询
     * @param field 字段名
     * @param value 值
     */
    public ESQueryBuilders term(String field, Object value) {
        list.add(new ESSimpleExpression (field, value, Operator.TERM).toBuilder());
        return this;
    }

    /**
     * 功能描述：Terms 查询
     * @param field 字段名
     * @param values 集合值
     */
    public ESQueryBuilders terms(String field, Collection<Object> values) {
        list.add(new ESSimpleExpression (field, values).toBuilder());
        return this;
    }

    /**
     * 功能描述：fuzzy 查询
     * @param field 字段名
     * @param value 值
     */
    public ESQueryBuilders fuzzy(String field, Object value) {
        list.add(new ESSimpleExpression (field, value, Operator.FUZZY).toBuilder());
        return this;
    }

    /**
     * 功能描述：Range 查询
     * @param from 起始值
     * @param to 末尾值
     */
    public ESQueryBuilders range(String field, Object from, Object to) {
        list.add(new ESSimpleExpression (field, from, to).toBuilder());
        return this;
    }


    /**
     * 功能描述：GTE 大于等于查询
     * @param
     */
    public ESQueryBuilders gte(String field, Object num) {
        list.add(new ESSimpleExpression (field, num,Operator.GTE).toBuilder());
        return this;
    }

    /**
     * 功能描述：LTE 小于等于查询
     * @param
     */
    public ESQueryBuilders lte(String field, Object num) {
        list.add(new ESSimpleExpression (field, num,Operator.LTE).toBuilder());
        return this;
    }

    /**
     * 功能描述：prefix 查询
     * @param field 字段名
     * @param value 值
     */
    public ESQueryBuilders prefix(String field, Object value) {
        list.add(new ESSimpleExpression (field, value, Operator.PREFIX).toBuilder());
        return this;
    }


  /*  *//**
     * 功能描述：Range 查询
     * @param queryString 查询语句
     *//*
    public ESQueryBuilders queryString(String queryString) {
        list.add(new ESSimpleExpression (queryString, Operator.QUERY_STRING).toBuilder());
        return this;
    }*/

    /**
     * 功能描述：Range 查询
     * @param
     */
    public ESQueryBuilders bool(BoolQueryBuilder boolQueryBuilder) {
        list.add(boolQueryBuilder);
        return this;
    }

    public ESQueryBuilders nested(NestedQueryBuilder nestedQueryBuilder) {
        list.add(nestedQueryBuilder);
        return this;
    }


    public List<QueryBuilder> listBuilders() {
        return list;
    }
}

package com.cn.shool.bigdata.bigdata.es.V2;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * @author: ""
 * @description: 条件接口
 * @Date:Created in 2019-10-
 */
public interface ESCriterion {
    public enum Operator {
        PREFIX,             /**根据字段前缀查询**/
        MATCH,              /**匹配查询**/
        MATCH_PHRASE,       /**精确匹配**/
        MULTI_MATCH,        /**多字段匹配**/


        TERM,               /**term查询**/
        TERMS,              /**term查询**/


        RANGE,              /**范围查询**/
        GTE,                 /**大于等于查询**/
        LTE,

        FUZZY,              /**根据字段前缀查询**/
        QUERY_STRING,       /**根据字段前缀查询**/
        MISSING ,            /**根据字段前缀查询**/

        BOOL
    }

    public enum MatchMode {
        START, END, ANYWHERE
    }

    public enum Projection {
        MAX, MIN, AVG, LENGTH, SUM, COUNT
    }

    public List<QueryBuilder> listBuilders();
}

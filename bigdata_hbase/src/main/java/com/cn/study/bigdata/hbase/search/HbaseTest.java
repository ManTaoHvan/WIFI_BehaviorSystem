package com.cn.shool.bigdata.bigdata.hbase.search;

import com.cn.shool.bigdata.bigdata.hbase.extractor.MapRowExtrator;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
public class HbaseTest {
    public static void main(String[] args) throws Exception {
        HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();

        String tableName = "test:relation";

        Get get = new Get("aa-aa-aa-aa-aa-aa".getBytes());

        hBaseSearchService.search(tableName,get,new MapRowExtrator());

    }
}

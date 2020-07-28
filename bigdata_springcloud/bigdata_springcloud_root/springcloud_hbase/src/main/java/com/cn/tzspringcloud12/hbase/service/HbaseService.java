package com.cn.shool.bigdata.hbase.service;

import com.cn.shool.bigdata.bigdata.hbase.entity.HBaseCell;
import com.cn.shool.bigdata.bigdata.hbase.entity.HBaseRow;
import com.cn.shool.bigdata.bigdata.hbase.extractor.MapRowExtrator;
import com.cn.shool.bigdata.bigdata.hbase.extractor.MultiVersionRowExtrator;
import com.cn.shool.bigdata.bigdata.hbase.extractor.SingleColumnMultiVersionRowExtrator;
import com.cn.shool.bigdata.bigdata.hbase.search.HBaseSearchService;
import com.cn.shool.bigdata.bigdata.hbase.search.HBaseSearchServiceImpl;
import org.apache.hadoop.hbase.client.Get;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
@Service
public class HbaseService {


    @Resource
    private HbaseService hbaseService;


    /**
     *  查找倒排索引biao
     * @param table
     * @param rowkey
     * @param versions
     * @return
     */
    public Set<String> getRowkeys(String table, String rowkey,int versions){
        Set<String> macSet =null;
        try {
            HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
            Get get = new Get(rowkey.getBytes());
            //设置HBASE多版本
            get.setMaxVersions(versions);
            //public SingleColumnMultiVersionRowExtrator(byte[] cf, byte[] cl, Set<String> values)
            SingleColumnMultiVersionRowExtrator singleColumnMultiVersionRowExtrator = new SingleColumnMultiVersionRowExtrator("cf".getBytes(), "phone_mac".getBytes(), new HashSet<>());
            macSet = hBaseSearchService.search(table, get, singleColumnMultiVersionRowExtrator);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return macSet;
    }


    /**
     *  通过任意一个条件查询出所有  关联信息
     * @param field
     * @param fieldValue
     * @return
     */
    public Map<String,List<String>> getRelation(String field, String fieldValue){

        Map<String,List<String>> map = new HashMap<>();

        //TODO 查倒索引表，获取主关联表的所有KEY
        String table = "test:"+field;  //表名
        String indexRowkey = fieldValue;
        //返回所有主关联表的rowkey
        Set<String> singleColumn = hbaseService.getRowkeys(table, indexRowkey, 1000);


        //TODO 2 根据rowkey查总关联表
        List<Map<String, String>> search = null;
        // 组装list<Get>
        List<Get> list = new ArrayList<>();

        //对所有的主关联表rowkey进行遍历，封装GET
        //TODO 封装LIST<GET>
        singleColumn.forEach(mac->{
            Get get = new Get(mac.getBytes());
            //需要多版本
            try {
                get.setMaxVersions(1000);
            } catch (IOException e) {
                e.printStackTrace();
            }
            list.add(get);
        });

        try {
            HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
            List<HBaseRow> searchResult = hBaseSearchService.search("test:relation", list, new MultiVersionRowExtrator());

            //多所有的cell遍历  将相同key的数据放到一个list里面去
            searchResult.forEach(hbaseRow ->{
                Map<String, Collection<HBaseCell>> cellMap = hbaseRow.getCell();
                cellMap.forEach((key,value)->{
                    List<String> listValue = new ArrayList<>();
                    value.forEach(x->{
                        listValue.add(x.toString());
                    });
                    map.put(key,listValue);
                });
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }


    /**
     *
     * @param field
     * @param rowkey
     * @return
     */
    public Map<String, String> getMap(String field,
                                      String rowkey){
        String tableName = "test:"+ field;
        HBaseSearchService hBaseSearchService = new HBaseSearchServiceImpl();
        Get get = new Get(rowkey.getBytes());
        Map<String, String> search = null;
        try {
            search = hBaseSearchService.search(tableName, get, new MapRowExtrator());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return search;
    }
}

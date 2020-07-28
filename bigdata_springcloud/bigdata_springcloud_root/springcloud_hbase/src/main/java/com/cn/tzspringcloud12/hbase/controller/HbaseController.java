package com.cn.shool.bigdata.hbase.controller;

import com.cn.shool.bigdata.bigdata.hbase.extractor.MapRowExtrator;
import com.cn.shool.bigdata.bigdata.hbase.search.HBaseSearchService;
import com.cn.shool.bigdata.bigdata.hbase.search.HBaseSearchServiceImpl;
import com.cn.shool.bigdata.hbase.service.HbaseService;
import com.cn.shool.bigdata.hbase.service.HbaseService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.hbase.client.Get;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
@Controller
@RequestMapping("/hbase")
@Api(value = "hbase查询接口")
public class HbaseController {

    @Resource
    private HbaseService hbaseService;


    @ApiOperation(value = "通过倒排索引获取主关联表ROWKEY",notes = "通过倒排索引获取主关联表ROWKEY")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "table",value = "hbase表名",required = true,dataType = "string"),
            @ApiImplicitParam(name = "rowkey",value = "hbase rowkey",required = true,dataType = "string"),
    })
    @ResponseBody
    @RequestMapping(value = "/getRowkeys",method = {RequestMethod.GET,RequestMethod.POST})
    public Set<String> getRowkeys(@RequestParam(name = "table") String table,
                                  @RequestParam(name = "rowkey") String rowkey){

        String tableName = "test:"+table;
        return hbaseService.getRowkeys(tableName,rowkey,100);
    }




    /**
     * 获取关联关系
     * @param field
     * @param fieldValue
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/getRelation",method = {RequestMethod.GET,RequestMethod.POST})
    public Map<String, List<String>> getRelation(@RequestParam(name="field") String field,
                                                 @RequestParam(name="fieldValue") String fieldValue){

        System.out.println("field====" +field);
        System.out.println("fieldValue====" +fieldValue);
        Map<String, List<String>> relation = hbaseService.getRelation(field, fieldValue);

        System.out.println(relation.toString());
        System.out.println(relation.size());

        return relation;
    }




    @ApiOperation(value = "获取hbase MAP数据",notes = "获取hbase MAP数据")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "field",value = "hbase表名",required = true,dataType = "string"),
            @ApiImplicitParam(name = "rowkey",value = "hbase rowkey",required = true,dataType = "string"),
    })
    @ResponseBody
    @RequestMapping(value = "/getMap",method = {RequestMethod.GET,RequestMethod.POST})
    public Map<String, String> getMap(@RequestParam(name = "field") String field,
                                 @RequestParam(name = "rowkey") String rowkey){
        return hbaseService.getMap(field,rowkey);
    }
}

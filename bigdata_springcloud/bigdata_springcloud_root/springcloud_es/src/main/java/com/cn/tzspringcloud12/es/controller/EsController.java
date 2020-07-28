package com.cn.shool.bigdata.es.controller;

import com.cn.shool.bigdata.bigdata.es.jest.jestservice.JestService;
import com.cn.shool.bigdata.bigdata.es.jest.jestservice.ResultParse;
import com.cn.shool.bigdata.es.feign.HbaseFeign;
import com.cn.shool.bigdata.es.service.EsService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
//@Controller一个控制器
@Controller
//路由
@RequestMapping("/es")
@Api(value = "全文检索接口", tags = "全文检索接口")
public class EsController {



    //引入hbase feign
    @Resource
    private HbaseFeign hbaseFeign;

    @Resource
    private EsService esService;

    /**
     * 基础查询
     * @param indexName     索引名
     * @param typeName      类型名
     * @param sortField     排序字段
     * @param sortValue      排序方式
     * @param pageNumber    页数
     * @param pageSize      没页数据大小
     * @return
     */
    //通用查询
    //ES一个基础查询
    //1 根据单个检索条件
    //2 分页  一页查多少条数据  查询条数  1.1000  2 分页 2. 10
    //3 排序  时间排序
    @ApiOperation(value = "根据单个字段过滤查询", notes = "根据单个字段过滤查询")
    @ResponseBody
    @RequestMapping(value = "/getBaseInfo", method = {RequestMethod.GET, RequestMethod.POST})
    public List<Map<String, Object>> getBaseInfo(@RequestParam(name="indexName") String indexName,
                                                 @RequestParam(name="typeName") String typeName,
                                                 @RequestParam(name="sortField") String sortField,
                                                 @RequestParam(name="sortValue") String sortValue,
                                                 @RequestParam(name="pageNumber") String pageNumber,
                                                 @RequestParam(name="pageSize") String pageSize){

        int pageNumber1 = Integer.valueOf(pageNumber);
        int pageSize1 = Integer.valueOf(pageSize);

        return esService.getBaseInfo(indexName,typeName,sortField,sortValue,pageNumber1,pageSize1);
    }


    /**
     * 根据单个条件   查询所有轨迹数据
     * @param field
     * @param fieldValue
     * @return
     */
    //通用查询
    //ES一个基础查询
    //1 根据单个检索条件
    //2 分页  一页查多少条数据  查询条数  1.1000  2 分页 2. 10
    //3 排序  时间排序
    @ApiOperation(value = "根据分类id获得分类列表", notes = "树列表")
    @ResponseBody
    @RequestMapping(value = "/getLocus", method = {RequestMethod.GET, RequestMethod.POST})
    public List<Map<String, Object>> getLocus(@RequestParam(name="field") String field,
                                                 @RequestParam(name="fieldValue") String fieldValue
                                            ){
        //第一步 首先要获取倒这个手机的所有MAC
        //从hbase钟查询  通过手机号获取所有MAC
        // hbase需要提供一个方法   通过一个字段  去找这个字段对应的所有MAC
        Set<String> singleColumn = hbaseFeign.getSingleColumn(field, fieldValue);
        Iterator<String> iterator = singleColumn.iterator();
        //while (iterator.hasNext()){
            String mac = iterator.next();
            System.out.println("调用hbase服务的getSingleColumn方法成功" + mac);
            //根据MAC查询所有轨迹
            List<Map<String, Object>> locus = esService.getLocus(mac);
       // }
        return locus;
    }

    //要通过所有的MAC找到轨迹
    // 根据MAC来查询




    /**
     * 根据任意数据类型条件查找轨迹数据
     * 测试jestAPI是否正常
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/test", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, Object> getTest(){
        Map<String, Object> stringObjectMap = null;
        try {
            // 构建 jestClient 客户端
            JestClient jestClient = JestService.getJestClient();
            // 调用
            JestResult jestResult = JestService.get(jestClient, "qq_20190508", "qq_20190508", "c0a800e1d08040e6bef0d5f18c1d3974");
            // 使用ResultParse 解析器
            stringObjectMap = ResultParse.parseGet(jestResult);
            System.out.println(jestResult);
            System.out.println(stringObjectMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stringObjectMap;
    }


    /**
     *  这里可以返回界面想要的数据，我们在这里面实现  查询hbase,ES ,HIVE 等接口就行了
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/print",method = {RequestMethod.GET,RequestMethod.POST})
    public String printHello(){
        return "hello";
    }

}

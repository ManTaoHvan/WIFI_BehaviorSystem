package com.cn.shool.bigdata.es.feign;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Set;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
@FeignClient(name = "tz12-springcloud-hbase")
public interface HbaseFeign {

    @ResponseBody
    @RequestMapping(value = "/hbase/getRowkeys", method = {RequestMethod.GET})
    Set<String> getSingleColumn(@RequestParam(name = "table") String table,
                                @RequestParam(name = "rowkey") String rowkey);

}

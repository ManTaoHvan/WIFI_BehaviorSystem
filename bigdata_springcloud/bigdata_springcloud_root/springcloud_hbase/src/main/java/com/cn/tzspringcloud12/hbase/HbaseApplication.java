package com.cn.shool.bigdata.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

/**
 * @author:
 * @description:
 * @Date:Created in 2019-10-
 */
//作为SpringBoot服务
@SpringBootApplication
//启动注册中心服务
@EnableEurekaClient
public class HbaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(HbaseApplication.class,args);
    }

}
